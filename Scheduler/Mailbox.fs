namespace Scheduler

open System
open System.Collections.Generic
open System.Threading.Tasks
open Microsoft.FSharp.Control

module Mailbox =
    open Job
    open DataLayer

    type internal Message =
        | Complete of Job
        | Failed of Job
        | QueueJobs of Job list

    let internal processJob (inbox: MailboxProcessor<Message>) (job: Job) evaluator =
        let task = new Task(fun _ ->
            job.SerializedTask
            |> Evaluator.deserialize
            |> evaluator
            Complete job |> inbox.Post
        )
        task.Start()


    // TODO: Options som CE?
    type Scheduler<'t> (dataLayer: IDataLayer<'t>, pollingInterval: TimeSpan, OnlyRunAfter: DateTime option, maxJobs: int, evaluator: 't -> unit) = // TODO TA INN OPTIONS? OnCompletedJob callback? Iaf maxjobs
        let mutable polling = false
        let mutable inFlight = 0
        // TODO: TEst at prio køa funker om man har ting med forskjellig prioritet i køa
        let queue = PriorityQueue<Job, DateTime option>()

        let rec poll (inbox: MailboxProcessor<Message>) (pollingInterval: TimeSpan) =
            async {
                do! Async.Sleep(pollingInterval)
                inbox.Post(QueueJobs (dataLayer.Get OnlyRunAfter))
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
                    | Complete job ->
                        dataLayer.SetDone job
                        inFlight <- inFlight - 1
                    | Failed job ->
                        dataLayer.SetFailed job
                        inFlight <- inFlight - 1
                    | QueueJobs jobs ->
                        queue.Clear ()
                        List.iter (fun j -> queue.Enqueue(j, j.OnlyRunAfter)) jobs

                    let rec dequeue () =
                        if (inFlight < maxJobs && queue.Count > 0) then
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
