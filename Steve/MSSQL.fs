namespace DataLayer

open System
open System.Data
open System.Threading.Tasks
open Dapper
open Microsoft.Data.SqlClient

module MSSQL =
    let private asTask (t: Task<unit>) : Task = t

    type MSSQL<'t>(connectionString: string) =
        let insertJob (transaction: IDbTransaction option) (job: Job.Job) : Task = task {
            let sql = "
                INSERT INTO Scheduled_Jobs (Id, Task, Status, OnlyRunAfter, LastUpdated)
                VALUES (@Id, @Task, @Status, @OnlyRunAfter, @LastUpdated)
            "

            let parameters = {|
                Id = job.Id
                Task = job.Task
                Status = job.Status
                OnlyRunAfter =
                    match job.OnlyRunAfter with
                    | Some v -> box v
                    | None -> null
                LastUpdated = DateTime.UtcNow
            |}
            match transaction with
            | Some transaction ->
                let! _ = transaction.Connection.ExecuteAsync(sql, parameters, transaction)
                ()
            | None ->
                use connection = new SqlConnection(connectionString)
                do! connection.OpenAsync()
                let! _ = connection.ExecuteAsync(sql, parameters)
                ()
        }

        let updateStatus (jobId: Guid) (status: Job.Status) : Task = task {
            use connection = new SqlConnection(connectionString)
            do! connection.OpenAsync()
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
                LastUpdated = DateTime.UtcNow
            |}
            let! _ = connection.ExecuteAsync(sql, parameters)
            ()
        }

        interface DataLayer.IDataLayer<'t> with
            member this.Setup () =
                asTask <| task {
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
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let! _ = connection.ExecuteAsync(sql)
                    ()
                }

            member this.Poll () = task {
                let sql = "
                    SELECT *
                    FROM Scheduled_Jobs
                    WHERE status = 'Waiting'
                    AND (onlyRunAfter IS NULL OR onlyRunAfter <= @now)
                "

                let parameters = {|
                    now = DateTime.UtcNow
                |}
                use connection = new SqlConnection(connectionString)
                do! connection.OpenAsync()
                let! results = connection.QueryAsync<Job.Job>(sql, parameters)
                return results |> Seq.toList
            }

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

    let create<'t> (connectionString: string) =
        let datalayer = new MSSQL<'t>(connectionString) :> DataLayer.IDataLayer<'t>
        datalayer.Setup().Wait()
        SqlMapper.AddTypeHandler(typeof<Job.Status>, Job.StatusHandler())
        datalayer
