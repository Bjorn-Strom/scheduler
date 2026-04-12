namespace DataLayer

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
        /// Gets all jobs in the pool
        /// </summary>
        abstract member Poll: unit -> Task<Job.Job list>

        /// <summary>
        /// Updates a specific job and marks it as done.
        /// Also update the last updated timestamp.
        /// </summary>
        /// <param name="job">The job to mark as done.</param>
        abstract member SetDone: Job.Job -> Task

        /// <summary>
        /// Updates a specific job and marks it as in-flight.
        /// Also update the last updated timestamp.
        /// </summary>
        /// <param name="job">The job to mark as in-flight.</param>
        abstract member SetInFlight: Job.Job -> Task

        /// <summary>
        /// Updates a specific job and marks it as in-flight.
        /// Also update the last updated timestamp.
        /// </summary>
        /// <param name="job">The job to mark as failed.</param>
        abstract member SetFailed: Job.Job -> Task

    let empty(): IDataLayer<'t> = {
        new IDataLayer<'t> with
            member this.Setup () = Task.CompletedTask
            member this.Register _ = Task.CompletedTask
            member this.Schedule _ _ = Task.CompletedTask
            member this.Poll () = Task.FromResult([])
            member this.SetDone _ = Task.CompletedTask
            member this.SetInFlight _ = Task.CompletedTask
            member this.SetFailed _ = Task.CompletedTask
            member this.RegisterSafe _ _ = Task.CompletedTask
            member this.ScheduleSafe _ _ _ = Task.CompletedTask
    }
