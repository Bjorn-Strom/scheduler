namespace Scheduler

open System
open System.Collections.Generic
open System.Threading.Tasks

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
    type Scheduler<'t> (dataLayer: IDataLayer<'t>, OnlyRunAfter: DateTime option, maxJobs: int, evaluator: 't -> unit) = // TODO TA INN OPTIONS? OnCompletedJob callback? Iaf maxjobs
        let mutable inFlight = 0
        let queue = PriorityQueue<Job, DateTime option>()

        let _ = MailboxProcessor.Start(fun inbox ->
            let rec loop () =
                async {
                    // TODO: Henter alt for ofte. Dette må man kunne tweake
                    inbox.Post(QueueJobs (dataLayer.Get OnlyRunAfter))
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
