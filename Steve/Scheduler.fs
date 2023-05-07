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
        if List.isEmpty (List.tail queue) then
            List.tryHead queue, []
        else
            List.tryHead queue, List.tail queue

[<RequireQualifiedAccess>]
module Scheduler =
    open Job
    open DataLayer.DataLayer

    type internal Message =
        | Completed of Job
        | Failed of Job
        | QueueJobs of Job list

    let internal processJob (inbox: MailboxProcessor<Message>) (job: Job) evaluator =
        let task = new Task(fun _ ->
            try
                job.Task
                |> Evaluator.deserialize
                |> evaluator
                Completed job |> inbox.Post
            with
                | _ -> Failed job |> inbox.Post
        )
        task.Start()

    type SchedulerSpec<'t> =
        { DataLayer: IDataLayer<'t>
          PollingInterval: TimeSpan
          MaxJobs: int option
          Evaluator: ('t -> unit) option }

    let empty<'t> (): SchedulerSpec<'t> =
        { DataLayer = empty()
          PollingInterval = TimeSpan.FromSeconds 0
          MaxJobs = None
          Evaluator = None }

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

            let rec poll (inbox: MailboxProcessor<Message>) (pollingInterval: TimeSpan) =
                async {
                    do! Async.Sleep(pollingInterval)
                    let polledJobs = config.DataLayer.Poll ()
                    inbox.Post(QueueJobs polledJobs)
                    polling <- false
                }

            MailboxProcessor.Start(fun inbox ->
                let rec loop () =
                    async {
                        if not polling then
                            polling <- true
                            Async.Start (poll inbox config.PollingInterval)

                        let! message = inbox.Receive()
                        match message with
                        | Completed job ->
                            config.DataLayer.SetDone job
                            inFlight <- inFlight - 1
                        | Failed job ->
                            config.DataLayer.SetFailed job
                            inFlight <- inFlight - 1
                        | QueueJobs jobs ->
                            queue <- JobQueue.enqueue jobs queue

                        let rec dequeue () =
                            // Only dequeue if there is room for another job and we have something in the queue
                            if ((config.MaxJobs.IsSome && inFlight < config.MaxJobs.Value) || config.MaxJobs.IsNone) && (List.length queue) > 0 then
                                let job, newQueue = JobQueue.dequeue queue
                                queue <- newQueue
                                if job.IsSome then
                                    config.DataLayer.SetInFlight(job.Value)
                                    inFlight <- inFlight + 1
                                    processJob inbox job.Value evaluator
                                    dequeue ()

                        dequeue ()
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

[<AutoOpen>]
module SchedulerBuilder =
    let schedulerBuilder<'t> () = Scheduler.SchedulerBuilder<'t>()
