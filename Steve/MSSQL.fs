namespace Steve

open System
open System.Data
open System.Text.RegularExpressions
open System.Threading.Tasks
open Dapper
open Microsoft.Data.SqlClient
open Steve.DataLayer

module MSSQL =
    do SqlMapper.AddTypeHandler(typeof<Job.Status>, Job.StatusHandler())

    // Error numbers SqlClient does not flag as IsTransient but that are safe to retry:
    // general network errors (0), connection drops, timeouts, deadlocks and Azure SQL throttling.
    let private transientErrorNumbers =
        Set.ofList [ -2; 0; 20; 64; 233; 1205; 4060; 10053; 10054; 10060; 10928; 10929; 11001; 40197; 40501; 40613; 49918; 49919; 49920 ]

    let internal isTransient (ex: SqlException) =
        ex.IsTransient || transientErrorNumbers.Contains ex.Number

    let internal withRetry (maxRetries: int) (baseDelayMs: float) (operation: unit -> Task<'a>) : Task<'a> =
        let rec attempt n = task {
            try
                return! operation()
            with
            | :? SqlException as ex when n < maxRetries && isTransient ex ->
                do! Task.Delay(TimeSpan.FromMilliseconds(baseDelayMs * float (pown 2 n)))
                return! attempt (n + 1)
        }
        attempt 0

    let private retry (operation: unit -> Task) : Task =
        withRetry 3 100.0 (fun () -> task { do! operation() }) :> Task

    let private retryResult (operation: unit -> Task<'a>) : Task<'a> =
        withRetry 3 100.0 operation

    let private validTableName = Regex(@"^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled)

    // Dapper cannot materialize F# anonymous records (no parameterless constructor),
    // so stat rows need a CLIMutable record.
    [<CLIMutable>]
    type internal StatusCountRow = { Status: Job.Status; Cnt: int }

    type MSSQL<'t>(connectionString: string, ?batchSize: int, ?tableName: string) =
        let batchSize = defaultArg batchSize 100
        let tableName = defaultArg tableName "Scheduled_Jobs"
        do
            if not (validTableName.IsMatch tableName) then
                invalidArg (nameof tableName) $"Invalid table name '{tableName}'. Only letters, digits and underscores are allowed."

        let insertJob (transaction: IDbTransaction option) (job: Job.JobRecord) : Task = task {
            let sql = $"
                INSERT INTO {tableName} (Id, Task, Status, OnlyRunAfter, LastUpdated, RetryCount, StartedAt, CompletedAt, ErrorMessage, DedupKey)
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

        let setDone (job: Job.JobRecord) : Task = task {
            use connection = new SqlConnection(connectionString)
            do! connection.OpenAsync()
            let sql = $"
                UPDATE {tableName}
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
            let sql = $"
                UPDATE {tableName}
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
                    let sql = $"
                        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tableName}')
                        BEGIN
                            CREATE TABLE {tableName} (
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

                        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{tableName}') AND name = 'StartedAt')
                        BEGIN
                            ALTER TABLE {tableName} ADD StartedAt DATETIME2(7) NULL;
                            ALTER TABLE {tableName} ADD CompletedAt DATETIME2(7) NULL;
                            ALTER TABLE {tableName} ADD ErrorMessage NVARCHAR(MAX) NULL;
                        END

                        -- Migrate existing TEXT columns to NVARCHAR(MAX)
                        IF EXISTS (SELECT * FROM sys.columns c JOIN sys.types t ON c.system_type_id = t.system_type_id
                                   WHERE c.object_id = OBJECT_ID('{tableName}') AND c.name = 'Task' AND t.name = 'text')
                        BEGIN
                            ALTER TABLE {tableName} ALTER COLUMN Task NVARCHAR(MAX) NOT NULL;
                        END

                        -- Migrate existing DATETIME columns to DATETIME2
                        IF EXISTS (SELECT * FROM sys.columns c JOIN sys.types t ON c.system_type_id = t.system_type_id
                                   WHERE c.object_id = OBJECT_ID('{tableName}') AND c.name = 'OnlyRunAfter' AND t.name = 'datetime')
                        BEGIN
                            ALTER TABLE {tableName} ALTER COLUMN OnlyRunAfter DATETIME2(7) NULL;
                            ALTER TABLE {tableName} ALTER COLUMN LastUpdated DATETIME2(7) NOT NULL;
                            ALTER TABLE {tableName} ALTER COLUMN StartedAt DATETIME2(7) NULL;
                            ALTER TABLE {tableName} ALTER COLUMN CompletedAt DATETIME2(7) NULL;
                        END

                        -- Index for Poll query
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{tableName}_Poll' AND object_id = OBJECT_ID('{tableName}'))
                        BEGIN
                            CREATE INDEX IX_{tableName}_Poll
                            ON {tableName} (Status, OnlyRunAfter)
                            WHERE Status = 'Waiting';
                        END

                        -- Index for dashboard ordering
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{tableName}_LastUpdated' AND object_id = OBJECT_ID('{tableName}'))
                        BEGIN
                            CREATE INDEX IX_{tableName}_LastUpdated
                            ON {tableName} (LastUpdated DESC);
                        END

                        -- Add DedupKey column
                        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('{tableName}') AND name = 'DedupKey')
                        BEGIN
                            ALTER TABLE {tableName} ADD DedupKey NVARCHAR(450) NULL;
                        END

                        -- Filtered unique index for deduplication (only active jobs)
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{tableName}_DedupKey' AND object_id = OBJECT_ID('{tableName}'))
                        BEGIN
                            CREATE UNIQUE INDEX IX_{tableName}_DedupKey
                            ON {tableName} (DedupKey)
                            WHERE DedupKey IS NOT NULL AND Status IN ('Waiting', 'InFlight');
                        END

                        -- Recurring job definitions
                        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{tableName}_Recurring')
                        BEGIN
                            CREATE TABLE {tableName}_Recurring (
                                Name NVARCHAR(200) PRIMARY KEY,
                                Task NVARCHAR(MAX) NOT NULL,
                                Schedule NVARCHAR(200) NOT NULL,
                                NextRun DATETIME2(7) NOT NULL,
                                LastEnqueued DATETIME2(7) NULL
                            )
                        END
                    "
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let! _ = connection.ExecuteAsync(sql)
                    ()
                })

            member this.Poll maxCount =
                if maxCount <= 0 then
                    Task.FromResult([])
                else
                    retryResult (fun () -> task {
                        let sql = $"
                            UPDATE TOP (@claimCount) {tableName} WITH (UPDLOCK, ROWLOCK, READPAST)
                            SET Status = 'InFlight', LastUpdated = @now, StartedAt = @now
                            OUTPUT inserted.*
                            WHERE Status = 'Waiting'
                            AND (onlyRunAfter IS NULL OR onlyRunAfter <= @now)
                        "
                        let parameters = {|
                            now = DateTime.UtcNow
                            claimCount = min maxCount batchSize
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

            member this.Release job =
                retry (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = $"
                        UPDATE {tableName}
                        SET Status = 'Waiting', StartedAt = NULL, LastUpdated = @Now
                        WHERE Id = @Id AND Status = 'InFlight'
                    "
                    let! _ = connection.ExecuteAsync(sql, {| Id = job.Id; Now = DateTime.UtcNow |})
                    ()
                })

            member this.SetRetry job runAfter =
                retry (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = $"
                        UPDATE {tableName}
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
                    let sql = $"
                        UPDATE {tableName}
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

            member this.UpsertRecurring job =
                retry (fun () -> task {
                    let now = DateTime.UtcNow
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    use transaction = connection.BeginTransaction()
                    let! existing =
                        connection.QuerySingleOrDefaultAsync<Recurring.RecurringRecord>(
                            $"SELECT * FROM {tableName}_Recurring WITH (UPDLOCK) WHERE Name = @Name",
                            {| Name = job.Name |}, transaction)
                    let serializedSchedule = Schedule.serialize job.Schedule
                    match box existing with
                    | null ->
                        let record = Recurring.createRecord job now
                        let! _ =
                            connection.ExecuteAsync(
                                $"INSERT INTO {tableName}_Recurring (Name, Task, Schedule, NextRun, LastEnqueued)
                                  VALUES (@Name, @Task, @Schedule, @NextRun, NULL)",
                                {| Name = record.Name; Task = record.Task; Schedule = record.Schedule; NextRun = record.NextRun |},
                                transaction)
                        ()
                    | _ when existing.Schedule = serializedSchedule ->
                        let! _ =
                            connection.ExecuteAsync(
                                $"UPDATE {tableName}_Recurring SET Task = @Task WHERE Name = @Name",
                                {| Name = job.Name; Task = Evaluator.serialize job.Task |}, transaction)
                        ()
                    | _ ->
                        let record = Recurring.createRecord job now
                        let! _ =
                            connection.ExecuteAsync(
                                $"UPDATE {tableName}_Recurring SET Task = @Task, Schedule = @Schedule, NextRun = @NextRun WHERE Name = @Name",
                                {| Name = record.Name; Task = record.Task; Schedule = record.Schedule; NextRun = record.NextRun |},
                                transaction)
                        ()
                    transaction.Commit()
                })

            member this.RemoveRecurring name =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let! rows =
                        connection.ExecuteAsync(
                            $"DELETE FROM {tableName}_Recurring WHERE Name = @Name", {| Name = name |})
                    return rows > 0
                })

            member this.EnqueueDueRecurring now =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    use transaction = connection.BeginTransaction()
                    // UPDLOCK + READPAST: concurrent workers skip definitions another
                    // worker is already firing, so occurrences are never double-enqueued.
                    let! due =
                        connection.QueryAsync<Recurring.RecurringRecord>(
                            $"SELECT * FROM {tableName}_Recurring WITH (UPDLOCK, ROWLOCK, READPAST) WHERE NextRun <= @Now",
                            {| Now = now |}, transaction)
                    let mutable enqueued = 0
                    for definition in due do
                        try
                            do! Recurring.occurrenceJob definition now |> insertJob (Some(transaction :> IDbTransaction))
                            enqueued <- enqueued + 1
                        with
                        | :? SqlException as ex when ex.Number = 2601 || ex.Number = 2627 ->
                            // Previous occurrence still active; skip this one.
                            ()
                        let advanced = Recurring.advance definition now
                        let! _ =
                            connection.ExecuteAsync(
                                $"UPDATE {tableName}_Recurring SET NextRun = @NextRun, LastEnqueued = @Now WHERE Name = @Name",
                                {| Name = definition.Name; NextRun = advanced.NextRun; Now = now |},
                                transaction)
                        ()
                    transaction.Commit()
                    return enqueued
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
                        SELECT COUNT(*) FROM {tableName} {statusFilter};
                        SELECT * FROM {tableName} {statusFilter}
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
                    let sql = $"SELECT * FROM {tableName} WHERE Id = @Id"
                    let! result = connection.QuerySingleOrDefaultAsync<Job.JobRecord>(sql, {| Id = id |})
                    return
                        if isNull (box result) then None
                        else Some result
                })

            member this.GetStats () =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = $"
                        SELECT Status, COUNT(*) as Cnt FROM {tableName} GROUP BY Status;
                        SELECT AVG(DATEDIFF(SECOND, StartedAt, CompletedAt) * 1.0) FROM {tableName} WHERE Status = 'Done' AND StartedAt IS NOT NULL AND CompletedAt IS NOT NULL;
                        SELECT COUNT(*) FROM {tableName} WHERE Status = 'Done' AND CompletedAt >= @OneHourAgo;
                    "
                    use! multi = connection.QueryMultipleAsync(sql, {| OneHourAgo = DateTime.UtcNow.AddHours(-1.0) |})
                    let! statusRows = multi.ReadAsync<StatusCountRow>()
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
                    let sql = $"
                        UPDATE {tableName}
                        SET Status = 'Waiting', ErrorMessage = NULL, OnlyRunAfter = NULL, LastUpdated = @Now
                        WHERE Id = @Id AND Status = 'Failed'
                    "
                    try
                        let! rows = connection.ExecuteAsync(sql, {| Id = id; Now = DateTime.UtcNow |})
                        return if rows > 0 then Succeeded else NotFound
                    with
                    | :? SqlException as ex when ex.Number = 2601 || ex.Number = 2627 ->
                        return DuplicateActive
                })

            member this.RequeueJob id =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = $"
                        UPDATE {tableName}
                        SET Status = 'Waiting', ErrorMessage = NULL, OnlyRunAfter = NULL,
                            StartedAt = NULL, CompletedAt = NULL, RetryCount = 0, LastUpdated = @Now
                        WHERE Id = @Id
                    "
                    try
                        let! rows = connection.ExecuteAsync(sql, {| Id = id; Now = DateTime.UtcNow |})
                        return if rows > 0 then Succeeded else NotFound
                    with
                    | :? SqlException as ex when ex.Number = 2601 || ex.Number = 2627 ->
                        return DuplicateActive
                })

            member this.DeleteJob id =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = $"DELETE FROM {tableName} WHERE Id = @Id"
                    let! rows = connection.ExecuteAsync(sql, {| Id = id |})
                    return rows > 0
                })

            member this.PurgeJobs status olderThan =
                retryResult (fun () -> task {
                    use connection = new SqlConnection(connectionString)
                    do! connection.OpenAsync()
                    let sql = $"DELETE FROM {tableName} WHERE Status = @Status AND LastUpdated < @Cutoff"
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
