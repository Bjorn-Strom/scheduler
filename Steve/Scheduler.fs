namespace Scheduler

open System
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Microsoft.FSharp.Core

[<RequireQualifiedAccess>]
module JobQueue =
    let enqueue (jobs: Job.Job list) queue =
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
    open DataLayer.DataLayer

    type internal Message =
        | Completed of Job
        | Failed of Job
        | QueueJobs of Job list
        | PollFailed of exn

    let internal processJob (inbox: MailboxProcessor<Message>) (job: Job) evaluator (onError: exn -> string -> unit) =
        let task = new Task(fun _ ->
            try
                job.Task
                |> Evaluator.deserialize
                |> evaluator
                Completed job |> inbox.Post
            with
                | ex ->
                    onError ex $"Job {job.Id}"
                    Failed job |> inbox.Post
        )
        task.Start()

    type SchedulerSpec<'t> =
        { DataLayer: IDataLayer<'t>
          PollingInterval: TimeSpan
          MaxJobs: int option
          Evaluator: ('t -> unit) option
          OnError: (exn -> string -> unit) option }

    let empty<'t> (): SchedulerSpec<'t> =
        { DataLayer = empty()
          PollingInterval = TimeSpan.FromSeconds 0
          MaxJobs = None
          Evaluator = None
          OnError = None }

    type SchedulerBuilder<'t>() =
        member _.Yield _: SchedulerSpec<'t> = empty<'t>()

        member _.Run(config: SchedulerSpec<'t>) =
            let mutable polling = false
            let mutable inFlight = 0
            let mutable queue: Job.Job list = []
            let evaluator =
                match config.Evaluator with
                | Some evaluator -> evaluator
                | None -> failwith "Evaluator must be supplied to schedulerBuilder"
            let onError =
                match config.OnError with
                | Some handler -> handler
                | None -> fun ex ctx -> eprintfn "[Steve] %s: %A" ctx ex

            let rec poll (inbox: MailboxProcessor<Message>) (pollingInterval: TimeSpan) =
                async {
                    do! Async.Sleep pollingInterval
                    try
                        let! polledJobs = config.DataLayer.Poll () |> Async.AwaitTask
                        inbox.Post(QueueJobs polledJobs)
                    with ex ->
                        inbox.Post(PollFailed ex)
                }

            MailboxProcessor.Start(fun inbox ->
                let rec dequeue () = async {
                    if ((config.MaxJobs.IsSome && inFlight < config.MaxJobs.Value) || config.MaxJobs.IsNone) && not (List.isEmpty queue) then
                        let job, newQueue = JobQueue.dequeue queue
                        queue <- newQueue
                        match job with
                        | Some j ->
                            try
                                do! config.DataLayer.SetInFlight j |> Async.AwaitTask
                                inFlight <- inFlight + 1
                                processJob inbox j evaluator onError
                            with ex ->
                                onError ex $"SetInFlight for job {j.Id}"
                            do! dequeue ()
                        | None -> ()
                }

                let rec loop () =
                    async {
                        if not polling then
                            polling <- true
                            Async.Start (poll inbox config.PollingInterval)

                        let! message = inbox.Receive()
                        match message with
                        | Completed job ->
                            try
                                do! config.DataLayer.SetDone job |> Async.AwaitTask
                            with ex ->
                                onError ex $"SetDone for job {job.Id}"
                            inFlight <- inFlight - 1
                        | Failed job ->
                            try
                                do! config.DataLayer.SetFailed job |> Async.AwaitTask
                            with ex ->
                                onError ex $"SetFailed for job {job.Id}"
                            inFlight <- inFlight - 1
                        | QueueJobs jobs ->
                            queue <- JobQueue.enqueue jobs queue
                            polling <- false
                        | PollFailed ex ->
                            onError ex "Poll"
                            polling <- false

                        do! dequeue ()
                        return! loop ()
                    }
                loop ()
                ) |> ignore
            ()

        /// <summary>
        /// Sets the data layer implementation to use in the scheduler configuration.
        /// </summary>
        /// <param name="datalayer">The `IDataLayer` implementation to use.</param>
        [<CustomOperation("with_datalayer")>]
        member x.Datalayer (config: SchedulerSpec<'t>, datalayer: IDataLayer<'t>) =
            { config with DataLayer = datalayer }

        /// <summary>
        /// Specifies how often to poll the datalayer for new jobs.
        /// Default is every 0 seconds
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
        member x.MaxJobs (config: SchedulerSpec<'t>, ?maxJobs: int) =
            { config with MaxJobs = maxJobs }

        /// <summary>
        /// Sets the evaluator to be used with the scheduler configuration.
        /// </summary>
        /// <param name="evaluator">The evaluator function.</param>
        /// <returns>The updated scheduler configuration.</returns>
        [<CustomOperation("with_evaluator")>]
        member x.Evaluator (config: SchedulerSpec<'t>, evaluator: 't -> unit) =
            { config with Evaluator = Some evaluator }

        /// <summary>
        /// Sets an error handler for the scheduler. Called when job execution,
        /// polling, or status updates fail. Receives the exception and a context string.
        /// Default: prints to stderr.
        /// </summary>
        /// <param name="handler">Error handler receiving (exception, context).</param>
        [<CustomOperation("with_on_error")>]
        member x.OnError (config: SchedulerSpec<'t>, handler: exn -> string -> unit) =
            { config with OnError = Some handler }

[<AutoOpen>]
module SchedulerBuilder =
    let schedulerBuilder<'t> () = Scheduler.SchedulerBuilder<'t>()
