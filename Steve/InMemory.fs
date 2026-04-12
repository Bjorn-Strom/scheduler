namespace DataLayer

open System
open System.Threading.Tasks

module InMemory =
    let canRunNow (now: DateTime) (job: Job.Job) =
        let isWaiting =
            job.Status = Job.Waiting

        let runAfterSatisfied =
            match job.OnlyRunAfter with
            | None -> true
            | Some t -> now >= t

        isWaiting && runAfterSatisfied

    type InMemory<'t>() =
        let mutable store: Job.Job list = []
        let lockObj = obj()
        interface DataLayer.IDataLayer<'t> with
            member this.Setup () = Task.CompletedTask
            member this.Register foo =
                let newJob = Job.create foo None
                lock lockObj (fun () -> store <- store @ [newJob])
                Task.CompletedTask
            member this.Schedule foo shouldRunAfter =
                let newJob = Job.create foo (Some shouldRunAfter)
                lock lockObj (fun () -> store <- store @ [newJob])
                Task.CompletedTask
            member this.Poll () =
                let now = DateTime.UtcNow
                let result = lock lockObj (fun () ->
                    store |> List.filter (canRunNow now))
                Task.FromResult(result)
            member this.SetDone job =
                lock lockObj (fun () ->
                    store <-
                        store
                        |> List.map (fun j ->
                            if j.Id = job.Id then
                                { j with Status = Job.Done; LastUpdated = DateTime.UtcNow }
                            else j))
                Task.CompletedTask
            member this.SetInFlight job =
                lock lockObj (fun () ->
                    store <-
                        store
                        |> List.map (fun j ->
                            if j.Id = job.Id then
                                { j with Status = Job.InFlight; LastUpdated = DateTime.UtcNow }
                            else j))
                Task.CompletedTask
            member this.SetFailed job =
                lock lockObj (fun () ->
                    store <-
                        store
                        |> List.map (fun j ->
                            if j.Id = job.Id then
                                { j with Status = Job.Failed; LastUpdated = DateTime.UtcNow }
                            else j))
                Task.CompletedTask

            member this.RegisterSafe foo _ =
                let newJob = Job.create foo None
                lock lockObj (fun () -> store <- store @ [newJob])
                Task.CompletedTask
            member this.ScheduleSafe foo shouldRunAfter _ =
                let newJob = Job.create foo (Some shouldRunAfter)
                lock lockObj (fun () -> store <- store @ [newJob])
                Task.CompletedTask

    let create<'t> () =
        let datalayer = InMemory<'t>() :> DataLayer.IDataLayer<'t>
        datalayer.Setup().Wait()
        datalayer
