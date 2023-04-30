namespace Scheduler

open System
open System.Collections.Generic
open System.Threading.Tasks
open DataLayer
open Microsoft.FSharp.Control

module Mailbox =
    open Job

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


    // TODO: Options som CE?
    type Scheduler<'t> (dataLayer: IDataLayer<'t>, pollingInterval: TimeSpan, pollingWindow: DateTime option, maxJobs: int option, evaluator: 't -> unit) = // TODO TA INN OPTIONS? OnCompletedJob callback? Iaf maxjobs
        let mutable polling = false
        let mutable inFlight = 0
        // TODO: TEst at prio køa funker om man har ting med forskjellig prioritet i køa
        let queue = PriorityQueue<Job, DateTime option>()

        let rec poll (inbox: MailboxProcessor<Message>) (pollingInterval: TimeSpan) =
            async {
                do! Async.Sleep(pollingInterval)
                inbox.Post(QueueJobs (dataLayer.Get pollingWindow))
                polling <- false
            }

        let _ = MailboxProcessor.Start(fun inbox ->
            let rec loop () =
                async {
                    if not polling then
                        polling <- true
                        Async.Start (poll inbox pollingInterval)

                    let! message = inbox.Receive()
                    match message with
                    | Completed job ->
                        dataLayer.SetDone job
                        inFlight <- inFlight - 1
                    | Failed job ->
                        dataLayer.SetFailed job
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
                        if ((maxJobs.IsSome && inFlight < maxJobs.Value) || maxJobs.IsNone) && queue.Count > 0 then
                            let job = queue.Dequeue()
                            dataLayer.SetInFlight(job)
                            inFlight <- inFlight + 1
                            processJob inbox job evaluator
                            dequeue ()

                    dequeue ()
                    return! loop ()
                }
            loop ()
            )

    type TestBuilderSpec<'t> =
        { DataLayer: IDataLayer<'t> option
          PollingInterval: TimeSpan
          PollingWindow: DateTime option
          MaxJobs: int option
          Evaluator: 't -> unit }

        static member Empty =
            { DataLayer = None
              PollingInterval = TimeSpan.FromSeconds 0
              PollingWindow = None
              MaxJobs = None
              Evaluator = id }

    type TestBuilder<'t>() =
        member _.Yield(_) = TestBuilderSpec<'t>.Empty

        member _.Run(config: TestBuilderSpec<'t>) =

            ()

        [<CustomOperation("use_datalayer")>]
        member x.Datalayer (config: TestBuilderSpec<'t>, datalayer: IDataLayer<'t>) =
            { config with DataLayer = Some datalayer }

    let testBuilder<'t> () = TestBuilder<'t>()
