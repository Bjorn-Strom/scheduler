namespace DataLayer

module DataLayer =
    open System
    open System.Data

    type IDataLayer<'t> =
        /// <summary>
        /// Does any required setup like creating tables in the database.
        /// </summary>
        abstract member Setup: unit -> unit

        /// <summary>
        /// Registers a job of type 't into the pool of jobs.
        /// This job will be performed ASAP.
        /// </summary>
        /// <param name="job">The item to register.</param>
        abstract member Register: 't -> unit

        /// <summary>
        /// Registers a job of type 't into the pool of jobs.
        /// This job will be performed ASAP.
        /// Jobs are only added if the transaction is committed
        /// </summary>
        /// <param name="job">The item to register.</param>
        /// <param name="transaction">The database transaction.</param>
        abstract member RegisterSafe: 't -> IDbTransaction -> unit

        /// <summary>
        /// Schedules an job of type 't to be performed at a given time.
        /// </summary>
        /// <param name="job">The item to schedule.</param>
        /// <param name="onlyRunAfter">The time after which the job will be performed.</param>
        abstract member Schedule: 't -> DateTime -> unit
        /// <summary>
        /// Schedules an job of type 't to be performed at a given time.
        /// Jobs are only added if the transaction is committed
        /// </summary>
        /// <param name="job">The item to schedule.</param>
        /// <param name="onlyRunAfter">The time after which the job will be performed.</param>
        /// <param name="transaction">The database transaction.</param>
        abstract member ScheduleSafe: 't -> DateTime -> IDbTransaction -> unit

        /// <summary>
        /// Gets all jobs in the pool.
        /// </summary>
        abstract member Poll: unit -> Job.Job list

        /// <summary>
        /// Updates a specific job and marks it as done.
        /// Also update the last updated timestamp.
        /// </summary>
        /// <param name="job">The job to mark as done.</param>
        abstract member SetDone: Job.Job -> unit

        /// <summary>
        /// Updates a specific job and marks it as in-flight.
        /// Also update the last updated timestamp.
        /// </summary>
        /// <param name="job">The job to mark as in-flight.</param>
        abstract member SetInFlight: Job.Job -> unit

        /// <summary>
        /// Updates a specific job and marks it as in-flight.
        /// Also update the last updated timestamp.
        /// </summary>
        /// <param name="job">The job to mark as failed.</param>
        abstract member SetFailed: Job.Job -> unit

    let empty(): IDataLayer<'t> = {
        new IDataLayer<'t> with
            member this.Setup () = ()
            member this.Register var0 = ()
            member this.Schedule var0 var1 = ()
            member this.Poll () = []
            member this.SetDone var0 = ()
            member this.SetInFlight var0 = ()
            member this.SetFailed var0 = ()
            member this.RegisterSafe var0 var1 = ()
            member this.ScheduleSafe var0 var1 var2 = ()
    }

