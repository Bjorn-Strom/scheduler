namespace Steve

module DataLayer =
    open System
    open System.Data
    open System.Threading.Tasks

    type IDataLayer<'t> =
        /// <summary>
        /// Does any required setup like creating tables in the database.
        /// </summary>
        abstract member Setup: unit -> Task

        /// <summary>
        /// Registers a job of type 't into the pool of jobs.
        /// This job will be performed ASAP.
        /// </summary>
        /// <param name="job">The item to register.</param>
        abstract member Register: 't -> Task

        /// <summary>
        /// Registers a job of type 't into the pool of jobs.
        /// This job will be performed ASAP.
        /// Jobs are only added if the transaction is committed
        /// </summary>
        /// <param name="job">The item to register.</param>
        /// <param name="transaction">The database transaction.</param>
        abstract member RegisterSafe: 't -> IDbTransaction -> Task

        /// <summary>
        /// Schedules an job of type 't to be performed at a given time.
        /// </summary>
        /// <param name="job">The item to schedule.</param>
        /// <param name="onlyRunAfter">The time after which the job will be performed.</param>
        abstract member Schedule: 't -> DateTime -> Task

        /// <summary>
        /// Schedules an job of type 't to be performed at a given time.
        /// Jobs are only added if the transaction is committed
        /// </summary>
        /// <param name="job">The item to schedule.</param>
        /// <param name="onlyRunAfter">The time after which the job will be performed.</param>
        /// <param name="transaction">The database transaction.</param>
        abstract member ScheduleSafe: 't -> DateTime -> IDbTransaction -> Task

        /// <summary>
        /// Atomically claims up to maxCount runnable jobs and marks them InFlight.
        /// Returns the claimed jobs. A maxCount of zero or less claims nothing.
        /// </summary>
        /// <param name="maxCount">The maximum number of jobs to claim.</param>
        abstract member Poll: int -> Task<Job.JobRecord list>

        /// <summary>
        /// Updates a specific job and marks it as done.
        /// Also update the last updated timestamp.
        /// </summary>
        /// <param name="job">The job to mark as done.</param>
        abstract member SetDone: Job.JobRecord -> Task

        /// <summary>
        /// Updates a specific job and marks it as in-flight.
        /// Also update the last updated timestamp.
        /// </summary>
        /// <param name="job">The job to mark as failed.</param>
        abstract member SetFailed: Job.JobRecord -> Task

        /// <summary>
        /// Releases a claimed (InFlight) job back to Waiting without counting
        /// it as a retry. Used when a job was claimed but never ran to completion,
        /// e.g. during graceful shutdown.
        /// </summary>
        /// <param name="job">The job to release.</param>
        abstract member Release: Job.JobRecord -> Task

        /// <summary>
        /// Resets a failed job back to Waiting for retry.
        /// Increments the retry count and sets OnlyRunAfter for backoff.
        /// </summary>
        /// <param name="job">The job to retry.</param>
        /// <param name="runAfter">When the job should next be eligible to run.</param>
        abstract member SetRetry: Job.JobRecord -> DateTime -> Task

        /// <summary>
        /// Reclaims InFlight jobs that have been running longer than the given timeout.
        /// Resets them back to Waiting so they can be re-processed.
        /// </summary>
        /// <param name="timeout">Jobs with StartedAt older than this duration are reclaimed.</param>
        abstract member ReclaimStale: TimeSpan -> Task<int>

        /// <summary>
        /// Registers a job with a deduplication key. If an active job (Waiting or InFlight)
        /// with the same key exists, returns false and does not insert.
        /// </summary>
        abstract member RegisterWithDedup: 't -> string -> Task<bool>

        /// <summary>
        /// Schedules a job with a deduplication key. If an active job (Waiting or InFlight)
        /// with the same key exists, returns false and does not insert.
        /// </summary>
        abstract member ScheduleWithDedup: 't -> DateTime -> string -> Task<bool>

        /// <summary>
        /// Inserts or updates a recurring job definition by name.
        /// If a definition with the same name and schedule exists, its next run time
        /// is preserved (only the task payload is updated); if the schedule changed,
        /// the next run time is recomputed from now.
        /// </summary>
        abstract member UpsertRecurring: Recurring.RecurringJob<'t> -> Task

        /// <summary>
        /// Removes a recurring job definition by name. Returns false if no such
        /// definition exists. Already-enqueued occurrences are unaffected.
        /// </summary>
        abstract member RemoveRecurring: string -> Task<bool>

        /// <summary>
        /// Enqueues a normal job for every recurring definition due at the given UTC time
        /// and advances each definition's next run. Occurrences carry a dedup key derived
        /// from the definition name, so a still-active previous occurrence suppresses the
        /// new one (the next run still advances). Returns the number of jobs enqueued.
        /// Called by the scheduler on every poll.
        /// </summary>
        abstract member EnqueueDueRecurring: DateTime -> Task<int>

    type JobQuery =
        { Status: Job.Status option
          Page: int
          PageSize: int }

    type JobStats =
        { WaitingCount: int
          InFlightCount: int
          DoneCount: int
          FailedCount: int
          AverageDurationSeconds: float option
          JobsCompletedLastHour: int }

    /// Outcome of putting a job back into the Waiting state from the dashboard.
    type JobActionResult =
        | Succeeded
        | NotFound
        /// The job has a dedup key and another Waiting/InFlight job already holds it.
        | DuplicateActive

    type IDashboardDataLayer =
        abstract member QueryJobs: JobQuery -> Task<Job.JobRecord list * int>
        abstract member GetJob: Guid -> Task<Job.JobRecord option>
        abstract member GetStats: unit -> Task<JobStats>
        abstract member RetryJob: Guid -> Task<JobActionResult>
        abstract member RequeueJob: Guid -> Task<JobActionResult>
        abstract member DeleteJob: Guid -> Task<bool>
        abstract member PurgeJobs: Job.Status -> TimeSpan -> Task<int>

    let empty(): IDataLayer<'t> = {
        new IDataLayer<'t> with
            member this.Setup () = Task.CompletedTask
            member this.Register _ = Task.CompletedTask
            member this.Schedule _ _ = Task.CompletedTask
            member this.Poll _ = Task.FromResult([])
            member this.SetDone _ = Task.CompletedTask
            member this.SetFailed _ = Task.CompletedTask
            member this.Release _ = Task.CompletedTask
            member this.RegisterSafe _ _ = Task.CompletedTask
            member this.ScheduleSafe _ _ _ = Task.CompletedTask
            member this.SetRetry _ _ = Task.CompletedTask
            member this.ReclaimStale _ = Task.FromResult(0)
            member this.RegisterWithDedup _ _ = Task.FromResult(true)
            member this.ScheduleWithDedup _ _ _ = Task.FromResult(true)
            member this.UpsertRecurring _ = Task.CompletedTask
            member this.RemoveRecurring _ = Task.FromResult(false)
            member this.EnqueueDueRecurring _ = Task.FromResult(0)
    }
