namespace DataLayer

// TODO: Make safe -> results i ytterpunkter. Inn fra DB
// TODO: Når connection open?
// TODO: README

open System
open System.Data
open Dapper
open Microsoft.Data.SqlClient

module MSSQL =
    type private Connection =
        | Connection of IDbConnection
        | Transaction of IDbTransaction

    type MSSQL<'t>(connection: SqlConnection) =
        let insertJob (connection: Connection) (job: Job.Job) =
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
            match connection with
            | Connection connection ->
                connection.Execute(sql, parameters) |> ignore
            | Transaction transaction ->
                transaction.Connection.Execute(sql, parameters, transaction) |> ignore

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

            member this.Get now =
                let sql = "
                    SELECT *
                    FROM Scheduled_Jobs
                    WHERE status = 'WAITING'
                    AND (onlyRunAfter IS NULL OR onlyRunAfter < @now)
                "

                let parameters = {|
                    now =
                        if now.IsSome then
                            box now.Value
                        else
                            null
                |}
                connection.Query<Job.Job>(sql, parameters)
                |> Seq.toList

            member this.Register toRegister =
                Job.create toRegister None
                |> insertJob (Connection connection)

            member this.RegisterSafe toRegister transaction =
                Job.create toRegister None
                |> insertJob (Transaction transaction)

            member this.Schedule toSchedule shouldRunAfter =
                Job.create toSchedule (Some shouldRunAfter)
                |> insertJob (Connection connection)

            member this.ScheduleSafe toSchedule shouldRunAfter transaction =
                Job.create toSchedule (Some shouldRunAfter)
                |> insertJob (Transaction transaction)
            member this.SetDone completedJob =
                updateStatus completedJob.Id Job.Status.Done
            member this.SetFailed failedJob =
                updateStatus failedJob.Id Job.Status.Failed
            member this.SetInFlight inFlightJob =
                updateStatus inFlightJob.Id Job.Status.InFlight

    let create<'t> (connection: SqlConnection) =
        let datalayer = MSSQL<'t>(connection) :> DataLayer.IDataLayer<'t>
        // Setup datalayer
        datalayer.Setup()
        SqlMapper.AddTypeHandler(typeof<Job.Status>, Job.StatusHandler())
        datalayer
