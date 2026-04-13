namespace Steve

open System
open System.Data
open System.Threading.Tasks
open Dapper
open Microsoft.Data.SqlClient
open Steve.DataLayer

module MSSQL =
    do SqlMapper.AddTypeHandler(typeof<Job.Status>, Job.StatusHandler())

    let private asTask (t: Task<unit>) : Task = t

    let internal withRetry (maxRetries: int) (baseDelayMs: float) (operation: unit -> Task<'a>) : Task<'a> =
        let rec attempt n = task {
            try
                return! operation()
            with
            | :? SqlException when n < maxRetries ->
                do! Task.Delay(TimeSpan.FromMilliseconds(baseDelayMs * float (pown 2 n)))
                return! attempt (n + 1)
        }
        attempt 0

    let private retry (operation: unit -> Task) : Task =
        withRetry 3 100.0 (fun () -> task { do! operation() }) :> Task

    let private retryResult (operation: unit -> Task<'a>) : Task<'a> =
        withRetry 3 100.0 operation

    type MSSQL<'t>(connectionString: string, ?batchSize: int) =
        let batchSize = defaultArg batchSize 100
        let insertJob (transaction: IDbTransaction option) (job: Job.JobRecord) : Task = task {
            let sql = "
                INSERT INTO Scheduled_Jobs (Id, Task, Status, OnlyRunAfter, LastUpdated, RetryCount, StartedAt, CompletedAt, ErrorMessage, DedupKey)
                VALUES (@Id, @Task, @Status, @OnlyRunAfter, @LastUpdated, @RetryCount, @StartedAt, @CompletedAt, @ErrorMessage, @DedupKey)
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
                RetryCount = job.RetryCount
                StartedAt =
                    match job.StartedAt with
                    | Some v -> box v
                    | None -> null
                CompletedAt =
                    match job.CompletedAt with
                    | Some v -> box v
                    | None -> null
                ErrorMessage =
                    match job.ErrorMessage with
                    | Some v -> box v
                    | None -> null
                DedupKey =
                    match job.DedupKey with
                    | Some v -> box v
                    | None -> null
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

        let setStatus (jobId: Guid) (status: Job.Status) : Task = task {
            use connection = new SqlConnection(connectionString)
            do! connection.OpenAsync()
            let sql = "
                UPDATE Scheduled_Jobs
                SET Status = @Status, LastUpdated = @LastUpdated
                WHERE Id = @Id
            "
            let parameters = {| Status = status; Id = jobId; LastUpdated = DateTime.UtcNow |}
            let! _ = connection.ExecuteAsync(sql, parameters)
            ()
        }

        let setDone (job: Job.JobRecord) : Task = task {
            use connection = new SqlConnection(connectionString)
            do! connection.OpenAsync()
            let sql = "
                UPDATE Scheduled_Jobs
                SET Status = 'Done', LastUpdated = @LastUpdated, CompletedAt = @CompletedAt
                WHERE Id = @Id
            "
            let parameters = {|
                Id = job.Id
                LastUpdated = DateTime.UtcNow
                CompletedAt =
                    match job.CompletedAt with
                    | Some v -> box v
                    | None -> box DateTime.UtcNow
            |}
            let! _ = connection.ExecuteAsync(sql, parameters)
            ()
        }

        let setFailed (job: Job.JobRecord) : Task = task {
            use connection = new SqlConnection(connectionString)
            do! connection.OpenAsync()
            let sql = "
                UPDATE Scheduled_Jobs
                SET Status = 'Failed', LastUpdated = @LastUpdated, ErrorMessage = @ErrorMessage
                WHERE Id = @Id
            "
            let parameters = {|
                Id = job.Id
                LastUpdated = DateTime.UtcNow
                ErrorMessage =
                    match job.ErrorMessage with
                    | Some v -> box v
                    | None -> null
            |}
            let! _ = connection.ExecuteAsync(sql, parameters)
            ()
        }

        interface IDataLayer<'t> with
            member this.Setup () =
                retry (fun () -> task {
                    let sql = "
                        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Scheduled_Jobs')
                        BEGIN
                            CREATE TABLE Scheduled_Jobs (
                                id UNIQUEIDENTIFIER PRIMARY KEY,
                                task NVARCHAR(MAX) NOT NULL,
                                status NVARCHAR(16) NOT NULL,
                                onlyRunAfter DATETIME2(7),
                                LastUpdated DATETIME2(7) NOT NULL,
                                RetryCount INT NOT NULL DEFAULT 0,
                                StartedAt DATETIME2(7) NULL,
                                CompletedAt DATETIME2(7) NULL,
                                ErrorMessage NVARCHAR(MAX) NULL,
                                DedupKey NVARCHAR(450) NULL
                            )
                        END

                        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Scheduled_Jobs') AND name = 'StartedAt')
                        BEGIN
                            ALTER TABLE Scheduled_Jobs ADD StartedAt DATETIME2(7) NULL;
                            ALTER TABLE Scheduled_Jobs ADD CompletedAt DATETIME2(7) NULL;
                            ALTER TABLE Scheduled_Jobs ADD ErrorMessage NVARCHAR(MAX) NULL;
                        END

                        -- Migrate existing TEXT columns to NVARCHAR(MAX)
                        IF EXISTS (SELECT * FROM sys.columns c JOIN sys.types t ON c.system_type_id = t.system_type_id
                                   WHERE c.object_id = OBJECT_ID('Scheduled_Jobs') AND c.name = 'Task' AND t.name = 'text')
                        BEGIN
                            ALTER TABLE Scheduled_Jobs ALTER COLUMN Task NVARCHAR(MAX) NOT NULL;
                        END

                        -- Migrate existing DATETIME columns to DATETIME2
                        IF EXISTS (SELECT * FROM sys.columns c JOIN sys.types t ON c.system_type_id = t.system_type_id
                                   WHERE c.object_id = OBJECT_ID('Scheduled_Jobs') AND c.name = 'OnlyRunAfter' AND t.name = 'datetime')
                        BEGIN
                            ALTER TABLE Scheduled_Jobs ALTER COLUMN OnlyRunAfter DATETIME2(7) NULL;
                            ALTER TABLE Scheduled_Jobs ALTER COLUMN LastUpdated DATETIME2(7) NOT NULL;
                            ALTER TABLE Scheduled_Jobs ALTER COLUMN StartedAt DATETIME2(7) NULL;
                            ALTER TABLE Scheduled_Jobs ALTER COLUMN CompletedAt DATETIME2(7) NULL;
                        END

                        -- Index for Poll query
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Scheduled_Jobs_Poll' AND object_id = OBJECT_ID('Scheduled_Jobs'))
                        BEGIN
                            CREATE INDEX IX_Scheduled_Jobs_Poll
                            ON Scheduled_Jobs (Status, OnlyRunAfter)
                            WHERE Status = 'Waiting';
                        END

                        -- Index for dashboard ordering
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Scheduled_Jobs_LastUpdated' AND object_id = OBJECT_ID('Scheduled_Jobs'))
                        BEGIN
                            CREATE INDEX IX_Scheduled_Jobs_LastUpdated
                            ON Scheduled_Jobs (LastUpdated DESC);
                        END

                        -- Add DedupKey column
                        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('Scheduled_Jobs') AND name = 'DedupKey')
                        BEGIN
                            ALTER TABLE Scheduled_Jobs ADD DedupKey NVARCHAR(450) NULL;
                        END

                        -- Filtered unique index for deduplication (only active jobs)
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_Scheduled_Jobs_DedupKey' AND object_id = OBJECT_ID('Scheduled_Jobs'))
                        BEGIN
                            CREATE UNIQUE INDEX IX_Scheduled_Jobs_DedupKey
                            ON Scheduled_Jobs (DedupKey)
                            WHERE DedupKey IS NOT NULL AND Status IN ('Waiting', 'InFlight');
                        END
                    "
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let! _ = connection.ExecuteAsync(sql)
                    ()
                })

            member this.Poll () =
                retryResult (fun () -> task {
                    let sql = "
                        UPDATE TOP (@batchSize) Scheduled_Jobs WITH (UPDLOCK, ROWLOCK, READPAST)
                        SET Status = 'InFlight', LastUpdated = @now, StartedAt = @now
                        OUTPUT inserted.*
                        WHERE Status = 'Waiting'
                        AND (onlyRunAfter IS NULL OR onlyRunAfter <= @now)
                    "
                    let parameters = {|
                        now = DateTime.UtcNow
                        batchSize = batchSize
                    |}
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let! results = connection.QueryAsync<Job.JobRecord>(sql, parameters)
                    return results |> Seq.toList
                })

            member this.Register toRegister =
                retry (fun () -> Job.create toRegister None |> insertJob None)

            member this.RegisterSafe toRegister transaction =
                Job.create toRegister None
                |> insertJob (Some transaction)

            member this.Schedule toSchedule shouldRunAfter =
                retry (fun () -> Job.create toSchedule (Some shouldRunAfter) |> insertJob None)

            member this.ScheduleSafe toSchedule shouldRunAfter transaction =
                Job.create toSchedule (Some shouldRunAfter)
                |> insertJob (Some transaction)

            member this.SetDone completedJob =
                retry (fun () -> setDone completedJob)

            member this.SetFailed failedJob =
                retry (fun () -> setFailed failedJob)

            member this.SetInFlight inFlightJob =
                retry (fun () -> setStatus inFlightJob.Id Job.Status.InFlight)

            member this.SetRetry job runAfter =
                retry (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = "
                        UPDATE Scheduled_Jobs
                        SET Status = 'Waiting',
                            RetryCount = @RetryCount,
                            OnlyRunAfter = @RunAfter,
                            LastUpdated = @LastUpdated
                        WHERE Id = @Id
                    "
                    let parameters = {|
                        Id = job.Id
                        RetryCount = job.RetryCount + 1
                        RunAfter = runAfter
                        LastUpdated = DateTime.UtcNow
                    |}
                    let! _ = connection.ExecuteAsync(sql, parameters)
                    ()
                })

            member this.ReclaimStale timeout =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = "
                        UPDATE Scheduled_Jobs
                        SET Status = 'Waiting', StartedAt = NULL, LastUpdated = @Now
                        WHERE Status = 'InFlight' AND StartedAt < @Cutoff
                    "
                    let parameters = {|
                        Now = DateTime.UtcNow
                        Cutoff = DateTime.UtcNow - timeout
                    |}
                    let! rows = connection.ExecuteAsync(sql, parameters)
                    return rows
                })

            member this.RegisterWithDedup toRegister dedupKey =
                retryResult (fun () -> task {
                    try
                        do! Job.createWithDedup toRegister None dedupKey |> insertJob None
                        return true
                    with
                    | :? SqlException as ex when ex.Number = 2601 || ex.Number = 2627 ->
                        return false
                })

            member this.ScheduleWithDedup toSchedule shouldRunAfter dedupKey =
                retryResult (fun () -> task {
                    try
                        do! Job.createWithDedup toSchedule (Some shouldRunAfter) dedupKey |> insertJob None
                        return true
                    with
                    | :? SqlException as ex when ex.Number = 2601 || ex.Number = 2627 ->
                        return false
                })

        interface IDashboardDataLayer with
            member this.QueryJobs query =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let statusFilter =
                        match query.Status with
                        | Some _ -> "WHERE Status = @Status"
                        | None -> ""
                    let sql = $"
                        SELECT COUNT(*) FROM Scheduled_Jobs {statusFilter};
                        SELECT * FROM Scheduled_Jobs {statusFilter}
                        ORDER BY LastUpdated DESC
                        OFFSET @Offset ROWS FETCH NEXT @PageSize ROWS ONLY;
                    "
                    let parameters = {|
                        Status =
                            match query.Status with
                            | Some s -> box (Job.Status.Serialize s)
                            | None -> null
                        Offset = max 0 ((query.Page - 1) * query.PageSize)
                        PageSize = query.PageSize
                    |}
                    use! multi = connection.QueryMultipleAsync(sql, parameters)
                    let! total = multi.ReadSingleAsync<int>()
                    let! jobs = multi.ReadAsync<Job.JobRecord>()
                    return (jobs |> Seq.toList, total)
                })

            member this.GetJob id =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = "SELECT * FROM Scheduled_Jobs WHERE Id = @Id"
                    let! result = connection.QuerySingleOrDefaultAsync<Job.JobRecord>(sql, {| Id = id |})
                    return
                        if isNull (box result) then None
                        else Some result
                })

            member this.GetStats () =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = "
                        SELECT Status, COUNT(*) as Cnt FROM Scheduled_Jobs GROUP BY Status;
                        SELECT AVG(DATEDIFF(SECOND, StartedAt, CompletedAt) * 1.0) FROM Scheduled_Jobs WHERE StartedAt IS NOT NULL AND CompletedAt IS NOT NULL;
                        SELECT COUNT(*) FROM Scheduled_Jobs WHERE Status = 'Done' AND CompletedAt >= @OneHourAgo;
                    "
                    use! multi = connection.QueryMultipleAsync(sql, {| OneHourAgo = DateTime.UtcNow.AddHours(-1.0) |})
                    let! statusRows = multi.ReadAsync<{| Status: Job.Status; Cnt: int |}>()
                    let! avgDuration = multi.ReadSingleOrDefaultAsync<float Nullable>()
                    let! lastHour = multi.ReadSingleAsync<int>()
                    let byStatus s =
                        statusRows |> Seq.tryFind (fun r -> r.Status = s)
                        |> Option.map (fun r -> r.Cnt) |> Option.defaultValue 0
                    return
                        { JobStats.WaitingCount = byStatus Job.Waiting
                          InFlightCount = byStatus Job.InFlight
                          DoneCount = byStatus Job.Done
                          FailedCount = byStatus Job.Failed
                          AverageDurationSeconds =
                            if avgDuration.HasValue then Some avgDuration.Value
                            else None
                          JobsCompletedLastHour = lastHour }
                })

            member this.RetryJob id =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = "
                        UPDATE Scheduled_Jobs
                        SET Status = 'Waiting', ErrorMessage = NULL, OnlyRunAfter = NULL, LastUpdated = @Now
                        WHERE Id = @Id AND Status = 'Failed'
                    "
                    let! rows = connection.ExecuteAsync(sql, {| Id = id; Now = DateTime.UtcNow |})
                    return rows > 0
                })

            member this.RequeueJob id =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = "
                        UPDATE Scheduled_Jobs
                        SET Status = 'Waiting', ErrorMessage = NULL, OnlyRunAfter = NULL,
                            StartedAt = NULL, CompletedAt = NULL, RetryCount = 0, LastUpdated = @Now
                        WHERE Id = @Id
                    "
                    let! rows = connection.ExecuteAsync(sql, {| Id = id; Now = DateTime.UtcNow |})
                    return rows > 0
                })

            member this.DeleteJob id =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = "DELETE FROM Scheduled_Jobs WHERE Id = @Id"
                    let! rows = connection.ExecuteAsync(sql, {| Id = id |})
                    return rows > 0
                })

            member this.PurgeJobs status olderThan =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = "DELETE FROM Scheduled_Jobs WHERE Status = @Status AND LastUpdated < @Cutoff"
                    let parameters = {|
                        Status = Job.Status.Serialize status
                        Cutoff = DateTime.UtcNow - olderThan
                    |}
                    let! rows = connection.ExecuteAsync(sql, parameters)
                    return rows
                })

    let create<'t> (connectionString: string) =
        let instance = new MSSQL<'t>(connectionString)
        (instance :> IDataLayer<'t>).Setup().Wait()
        instance :> IDataLayer<'t>

    let createWithBatchSize<'t> (connectionString: string) (batchSize: int) =
        let instance = new MSSQL<'t>(connectionString, batchSize)
        (instance :> IDataLayer<'t>).Setup().Wait()
        instance :> IDataLayer<'t>

    let createAsync<'t> (connectionString: string) = task {
        let instance = new MSSQL<'t>(connectionString)
        do! (instance :> IDataLayer<'t>).Setup()
        return instance :> IDataLayer<'t>
    }

    let createAsyncWithBatchSize<'t> (connectionString: string) (batchSize: int) = task {
        let instance = new MSSQL<'t>(connectionString, batchSize)
        do! (instance :> IDataLayer<'t>).Setup()
        return instance :> IDataLayer<'t>
    }
