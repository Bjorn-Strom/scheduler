namespace Steve

open System
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Microsoft.Extensions.Logging.Abstractions
open Microsoft.FSharp.Control
open Microsoft.FSharp.Core

[<RequireQualifiedAccess>]
module JobQueue =
    let enqueue (jobs: Job.JobRecord list) queue =
        queue @ jobs
        |> List.distinctBy (fun x -> x.Id)
        |> List.sortBy (fun x -> x.OnlyRunAfter)

    let dequeue queue =
        match queue with
        | [] -> None, []
        | [x] -> Some x, []
        | x :: rest -> Some x, rest

[<RequireQualifiedAccess>]
module Scheduler =
    open Job
    open Steve.DataLayer

    type internal Message =
        | Completed of JobRecord
        | Failed of JobRecord * string option
        | QueueJobs of JobRecord list
        | PollFailed of exn
        | Shutdown of TaskCompletionSource<unit>

    let internal processJob (inbox: MailboxProcessor<Message>) (job: JobRecord) (evaluator: 't -> CancellationToken -> Task) (ct: CancellationToken) (logger: ILogger) =
        let work = Func<Task>(fun () -> task {
            try
                logger.LogDebug("Job {JobId} started", job.Id)
                do! job.Task |> Evaluator.deserialize |> fun t -> evaluator t ct
                logger.LogDebug("Job {JobId} completed", job.Id)
                Completed job |> inbox.Post
            with
            | :? OperationCanceledException when ct.IsCancellationRequested ->
                logger.LogInformation("Job {JobId} cancelled during shutdown", job.Id)
                Completed job |> inbox.Post
            | ex ->
                logger.LogError(ex, "Job {JobId} failed", job.Id)
                Failed (job, Some (ex.ToString())) |> inbox.Post
        })
        Task.Run(work) |> ignore

    type SchedulerHandle internal (stop: unit -> Task) =
        /// <summary>
        /// Stops the scheduler. No new jobs will be started.
        /// Returns a Task that completes when all in-flight jobs finish.
        /// </summary>
        member _.Stop() = stop()
        interface IAsyncDisposable with
            member this.DisposeAsync() = ValueTask(this.Stop())

    type SchedulerSpec<'t> =
        { DataLayer: IDataLayer<'t>
          PollingInterval: TimeSpan
          MaxJobs: int option
          MaxRetries: int
          StaleTimeout: TimeSpan option
          Evaluator: ('t -> CancellationToken -> Task) option
          Logger: ILogger option }

    let empty<'t> (): SchedulerSpec<'t> =
        { DataLayer = empty()
          PollingInterval = TimeSpan.FromSeconds 5
          MaxJobs = None
          MaxRetries = 0
          StaleTimeout = None
          Evaluator = None
          Logger = None }

    type SchedulerBuilder<'t>() =
        member _.Yield _: SchedulerSpec<'t> = empty<'t>()

        member _.Run(config: SchedulerSpec<'t>) : SchedulerHandle =
            let evaluator =
                match config.Evaluator with
                | Some evaluator -> evaluator
                | None -> failwith "Evaluator must be supplied to schedulerBuilder"
            let logger =
                match config.Logger with
                | Some l -> l
                | None -> NullLogger.Instance
            let cts = new CancellationTokenSource()

            let retryBackoff (retryCount: int) =
                DateTime.UtcNow.AddSeconds(float (pown 2 retryCount))

            let handleFailed (job: JobRecord) = async {
                if job.RetryCount < config.MaxRetries then
                    let runAfter = retryBackoff job.RetryCount
                    logger.LogWarning("Job {JobId} failed, scheduling retry {RetryNum}/{MaxRetries} after {RunAfter}", job.Id, job.RetryCount + 1, config.MaxRetries, runAfter)
                    try
                        do! config.DataLayer.SetRetry job runAfter |> Async.AwaitTask
                    with ex ->
                        logger.LogError(ex, "SetRetry failed for job {JobId}", job.Id)
                else
                    logger.LogError("Job {JobId} failed after {RetryCount} retries, marking as Failed", job.Id, job.RetryCount)
                    try
                        do! config.DataLayer.SetFailed job |> Async.AwaitTask
                    with ex ->
                        logger.LogError(ex, "SetFailed failed for job {JobId}", job.Id)
            }

            let rec poll (inbox: MailboxProcessor<Message>) (pollingInterval: TimeSpan) =
                async {
                    do! Async.Sleep pollingInterval
                    try
                        match config.StaleTimeout with
                        | Some timeout ->
                            let! reclaimed = config.DataLayer.ReclaimStale timeout |> Async.AwaitTask
                            if reclaimed > 0 then
                                logger.LogWarning("Reclaimed {Count} stale InFlight job(s)", reclaimed)
                        | None -> ()
                        let! polledJobs = config.DataLayer.Poll () |> Async.AwaitTask
                        if not polledJobs.IsEmpty then
                            logger.LogInformation("Poll returned {JobCount} job(s)", polledJobs.Length)
                        inbox.Post(QueueJobs polledJobs)
                    with ex ->
                        inbox.Post(PollFailed ex)
                }

            let inbox = MailboxProcessor.Start(fun inbox ->
                let rec dequeue inFlight queue = async {
                    if ((config.MaxJobs.IsSome && inFlight < config.MaxJobs.Value) || config.MaxJobs.IsNone) && not (List.isEmpty queue) then
                        let job, newQueue = JobQueue.dequeue queue
                        match job with
                        | Some j ->
                            processJob inbox j evaluator cts.Token logger
                            return! dequeue (inFlight + 1) newQueue
                        | None -> return inFlight, queue
                    else
                        return inFlight, queue
                }

                let rec drain (tcs: TaskCompletionSource<unit>) inFlight =
                    async {
                        if inFlight <= 0 then
                            logger.LogInformation("Scheduler stopped")
                            tcs.TrySetResult() |> ignore
                        else
                            let! message = inbox.Receive()
                            match message with
                            | Completed job ->
                                let doneJob = { job with CompletedAt = Some DateTime.UtcNow }
                                try
                                    do! config.DataLayer.SetDone doneJob |> Async.AwaitTask
                                with ex ->
                                    logger.LogError(ex, "SetDone failed for job {JobId}", job.Id)
                                return! drain tcs (inFlight - 1)
                            | Failed (job, errorMsg) ->
                                do! handleFailed { job with ErrorMessage = errorMsg }
                                return! drain tcs (inFlight - 1)
                            | _ -> return! drain tcs inFlight
                    }

                let rec loop polling inFlight queue =
                    async {
                        let polling =
                            if not polling then
                                Async.Start (poll inbox config.PollingInterval)
                                true
                            else polling

                        let! message = inbox.Receive()
                        match message with
                        | Completed job ->
                            let doneJob = { job with CompletedAt = Some DateTime.UtcNow }
                            try
                                do! config.DataLayer.SetDone doneJob |> Async.AwaitTask
                            with ex ->
                                logger.LogError(ex, "SetDone failed for job {JobId}", job.Id)
                            let! inFlight, queue = dequeue (inFlight - 1) queue
                            return! loop polling inFlight queue
                        | Failed (job, errorMsg) ->
                            do! handleFailed { job with ErrorMessage = errorMsg }
                            let! inFlight, queue = dequeue (inFlight - 1) queue
                            return! loop polling inFlight queue
                        | QueueJobs jobs ->
                            let newQueue = JobQueue.enqueue jobs queue
                            let! inFlight, newQueue = dequeue inFlight newQueue
                            return! loop false inFlight newQueue
                        | PollFailed ex ->
                            logger.LogError(ex, "Poll failed")
                            let! inFlight, queue = dequeue inFlight queue
                            return! loop false inFlight queue
                        | Shutdown tcs ->
                            logger.LogInformation("Scheduler shutting down, {InFlight} job(s) in flight", inFlight)
                            cts.Cancel()
                            return! drain tcs inFlight
                    }
                loop false 0 []
                )
            inbox.Error.Add(fun ex ->
                logger.LogCritical(ex, "Scheduler MailboxProcessor crashed with unhandled exception"))
            let tcs = TaskCompletionSource<unit>()
            SchedulerHandle(fun () ->
                inbox.Post(Shutdown tcs)
                tcs.Task :> Task)

        /// <summary>
        /// Sets the data layer implementation to use in the scheduler configuration.
        /// </summary>
        /// <param name="datalayer">The `IDataLayer` implementation to use.</param>
        [<CustomOperation("with_datalayer")>]
        member x.Datalayer (config: SchedulerSpec<'t>, datalayer: IDataLayer<'t>) =
            { config with DataLayer = datalayer }

        /// <summary>
        /// Specifies how often to poll the datalayer for new jobs.
        /// Default is every 5 seconds
        /// </summary>
        /// <param name="pollingInterval">The interval for polling new jobs.</param>
        [<CustomOperation("with_polling_interval")>]
        member x.PollingInterval (config: SchedulerSpec<'t>, pollingInterval: TimeSpan) =
            { config with PollingInterval = pollingInterval  }

        /// <summary>
        /// Sets the maximum number of jobs that can run concurrently in the scheduler configuration.
        /// If nothing is supplied it will attempt to run as all the jobs it can.
        /// </summary>
        /// <param name="maxJobs">The maximum number of jobs.</param>
        [<CustomOperation("with_max_jobs")>]
        member x.MaxJobs (config: SchedulerSpec<'t>, maxJobs: int) =
            { config with MaxJobs = Some maxJobs }

        /// <summary>
        /// Sets the maximum number of times a failed job will be retried.
        /// Retries use exponential backoff (2^n seconds). Default: 0 (no retries).
        /// </summary>
        /// <param name="maxRetries">The maximum retry count.</param>
        [<CustomOperation("with_max_retries")>]
        member x.MaxRetries (config: SchedulerSpec<'t>, maxRetries: int) =
            { config with MaxRetries = maxRetries }

        /// <summary>
        /// Sets a timeout for stale InFlight jobs. Jobs that have been InFlight
        /// longer than this duration are reset to Waiting and re-processed.
        /// Useful for recovering from crashed workers.
        /// </summary>
        /// <param name="timeout">The stale job timeout.</param>
        [<CustomOperation("with_stale_timeout")>]
        member x.StaleTimeout (config: SchedulerSpec<'t>, timeout: TimeSpan) =
            { config with StaleTimeout = Some timeout }

        /// <summary>
        /// Sets the evaluator function that processes jobs.
        /// The function receives a CancellationToken that is triggered on scheduler shutdown.
        /// </summary>
        /// <param name="evaluator">The evaluator function.</param>
        [<CustomOperation("with_evaluator")>]
        member x.Evaluator (config: SchedulerSpec<'t>, evaluator: 't -> CancellationToken -> Task) =
            { config with Evaluator = Some evaluator }

        /// <summary>
        /// Sets an ILogger for the scheduler. Logs job lifecycle events,
        /// poll results, and errors. Default: NullLogger (no output).
        /// </summary>
        /// <param name="logger">The ILogger instance.</param>
        [<CustomOperation("with_logger")>]
        member x.Logger (config: SchedulerSpec<'t>, logger: ILogger) =
            { config with Logger = Some logger }

[<AutoOpen>]
module SchedulerBuilder =
    let schedulerBuilder<'t> () = Scheduler.SchedulerBuilder<'t>()
