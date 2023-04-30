namespace Scheduler

open System
open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.FSharp.Control

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
                job.SerializedTask
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
          PollingWindow: DateTime option
          MaxJobs: int option
          Evaluator: 't -> unit }

    let empty<'t> (): SchedulerSpec<'t> =
        { DataLayer = empty()
          PollingInterval = TimeSpan.FromSeconds 0
          PollingWindow = None
          MaxJobs = None
          Evaluator = fun _ -> () }

    type SchedulerBuilder<'t>() =
        member _.Yield _: SchedulerSpec<'t> = empty<'t>()

        member _.Run(config: SchedulerSpec<'t>) =
            // Setup datalayer
            config.DataLayer.Setup()

            let mutable polling = false
            let mutable inFlight = 0
            // TODO: TEst at prio køa funker om man har ting med forskjellig prioritet i køa
            let queue = PriorityQueue<Job, DateTime option>()

            let rec poll (inbox: MailboxProcessor<Message>) (pollingInterval: TimeSpan) =
                async {
                    do! Async.Sleep(pollingInterval)
                    inbox.Post(QueueJobs (config.DataLayer.Get config.PollingWindow))
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
                            // Only add new jobs to the queue
                            jobs
                            |> List.iter (fun j ->
                                let exists, _ = queue.TryPeek(ref j)
                                if not exists then
                                    queue.Enqueue(j, j.OnlyRunAfter)
                            )

                        let rec dequeue () =
                            // Only dequeue if there is room for another job and we have something in the queue
                            if ((config.MaxJobs.IsSome && inFlight < config.MaxJobs.Value) || config.MaxJobs.IsNone) && queue.Count > 0 then
                                let job = queue.Dequeue()
                                config.DataLayer.SetInFlight(job)
                                inFlight <- inFlight + 1
                                processJob inbox job config.Evaluator
                                dequeue ()

                        dequeue ()
                        return! loop ()
                    }
                loop ()
                ) |> ignore
            ()

        [<CustomOperation("with_datalayer")>]
        member x.Datalayer (config: SchedulerSpec<'t>, datalayer: IDataLayer<'t>) =
            { config with DataLayer = datalayer }

        [<CustomOperation("with_polling_interval")>]
        member x.PollingInterval (config: SchedulerSpec<'t>, pollingInterval: TimeSpan) =
            { config with PollingInterval = pollingInterval  }

        [<CustomOperation("with_polling_window")>]
        member x.PollingWindow (config: SchedulerSpec<'t>, ?pollingWindow: DateTime) =
            { config with PollingWindow =  pollingWindow  }

        [<CustomOperation("with_max_jobs")>]
        member x.MaxJobs (config: SchedulerSpec<'t>, ?maxJobs: int) =
            { config with MaxJobs = maxJobs }

        [<CustomOperation("with_evaluator")>]
        member x.Evaluator (config: SchedulerSpec<'t>, evaluator: 't -> unit) =
            { config with Evaluator = evaluator }

[<AutoOpen>]
module SchedulerBuilder =
    let schedulerBuilder<'t> () = Scheduler.SchedulerBuilder<'t>()
