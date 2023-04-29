namespace Scheduler

open System
open System.Collections.Generic
open System.Threading.Tasks

module Mailbox =
    open Job
    open DataLayer

    type internal Message =
        | Complete of Job
        | QueueJobs of Job list

    let internal processJob (inbox: MailboxProcessor<Message>) (job: Job) evaluator =
        let task = new Task(fun _ ->
            job.SerializedTask
            |> Evaluator.deserialize
            |> evaluator
            // TODO: Når burde denne kalles?
            Complete job |> inbox.Post
        )
        task.Start()

    // TODO: Options som CE?
    type Scheduler<'t> (dataLayer: IDataLayer<'t>, maxJobs: int, evaluator: 't -> unit) = // TODO TA INN OPTIONS? OnCompletedJob callback? Iaf maxjobs
        let mutable inFlight = 0
        // TODO: Kø er litt problematisk
        // TODO: Dersom køa er full, men alle jobbene i køa skal kjøres om 3 dager vil ingen som skal kjøre NÅ kjøres
        // TODO: Hva med den nye priority køa til C#, prioriter etter kortest tid
        let queue = Queue<Job>()

        let _ = MailboxProcessor.Start(fun inbox ->
            let rec loop () =
                async {
                    // TODO: Henter alt for ofte. Dette må man kunne tweake
                    inbox.Post(QueueJobs (dataLayer.Get DateTime.Now))
                    let! message = inbox.Receive()
                    match message with
                    | Complete job ->
                        dataLayer.Update job
                        inFlight <- inFlight - 1
                    | QueueJobs jobs ->
                        queue.Clear ()
                        List.iter queue.Enqueue jobs
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