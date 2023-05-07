namespace DataLayer

open System
open System.Data
open Dapper
open Microsoft.Data.SqlClient

module MSSQL =
    type MSSQL<'t>(connectionString: string) =
        let connection =
            let connection = new SqlConnection(connectionString)
            connection.Open()
            connection

        let insertJob (transaction: IDbTransaction option) (job: Job.Job) =
            let sql = "
                INSERT INTO Scheduled_Jobs (Id, Task, Status, OnlyRunAfter, LastUpdated)
                VALUES (@Id, @Task, @Status, @OnlyRunAfter, @LastUpdated)
            "
            let parameters = {|
                Id = job.Id
                Task = job.Task
                Status = job.Status
                OnlyRunAfter =
                    if job.OnlyRunAfter.IsSome then
                        box job.OnlyRunAfter.Value
                    else
                        null
                LastUpdated = DateTime.Now
            |}
            match transaction with
            | Some transaction ->
                transaction.Connection.Execute(sql, parameters, transaction) |> ignore
            | None ->
                connection.Execute(sql, parameters) |> ignore

        let updateStatus (jobId: Guid) (status: Job.Status) =
            let sql = "
                UPDATE Scheduled_Jobs
                SET
                    Status = @Status,
                    LastUpdated = @LastUpdated
                WHERE Id = @Id
            "
            let parameters = {|
                Status = status
                Id = jobId
                LastUpdated = DateTime.Now
            |}
            connection.Execute(sql, parameters) |> ignore

        interface DataLayer.IDataLayer<'t> with
            member this.Setup () =
                let sql = "
                    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Scheduled_Jobs')
                    BEGIN
                        CREATE TABLE Scheduled_Jobs (
                            id UNIQUEIDENTIFIER PRIMARY KEY,
                            task TEXT NOT NULL,
                            status NVARCHAR(16) NOT NULL,
                            onlyRunAfter DATETIME,
                            LastUpdated DATETIME NOT NULL
                        )
                    END
                "
                connection.Execute(sql) |> ignore

            member this.Poll () =
                let sql = "
                    SELECT *
                    FROM Scheduled_Jobs
                    WHERE status = 'WAITING'
                    AND (onlyRunAfter IS NULL OR onlyRunAfter < @now)
                "

                let parameters = {|
                    now = DateTime.Now
                |}
                connection.Query<Job.Job>(sql, parameters)
                |> Seq.toList

            member this.Register toRegister =
                Job.create toRegister None
                |> insertJob None

            member this.RegisterSafe toRegister transaction =
                Job.create toRegister None
                |> insertJob (Some transaction)

            member this.Schedule toSchedule shouldRunAfter =
                Job.create toSchedule (Some shouldRunAfter)
                |> insertJob None

            member this.ScheduleSafe toSchedule shouldRunAfter transaction =
                Job.create toSchedule (Some shouldRunAfter)
                |> insertJob (Some transaction)
            member this.SetDone completedJob =
                updateStatus completedJob.Id Job.Status.Done
            member this.SetFailed failedJob =
                updateStatus failedJob.Id Job.Status.Failed
            member this.SetInFlight inFlightJob =
                updateStatus inFlightJob.Id Job.Status.InFlight

        interface IDisposable with
            member this.Dispose() =
                connection.Close()

    let create<'t> (connectionString: string) =
        let datalayer = new MSSQL<'t>(connectionString) :> DataLayer.IDataLayer<'t>
        // Setup datalayer
        datalayer.Setup()
        SqlMapper.AddTypeHandler(typeof<Job.Status>, Job.StatusHandler())
        datalayer
