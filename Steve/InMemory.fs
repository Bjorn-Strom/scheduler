namespace Steve

open System
open System.Threading.Tasks
open Steve.DataLayer

module InMemory =
    let canRunNow (now: DateTime) (job: Job.JobRecord) =
        let isWaiting =
            job.Status = Job.Waiting

        let runAfterSatisfied =
            match job.OnlyRunAfter with
            | None -> true
            | Some t -> now >= t

        isWaiting && runAfterSatisfied

    type InMemory<'t>() =
        let mutable store: Job.JobRecord list = []
        let lockObj = obj()
        interface IDataLayer<'t> with
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
                    let ready = store |> List.filter (canRunNow now)
                    store <-
                        store
                        |> List.map (fun j ->
                            if ready |> List.exists (fun r -> r.Id = j.Id) then
                                { j with Status = Job.InFlight; LastUpdated = now; StartedAt = Some now }
                            else j)
                    ready |> List.map (fun j -> { j with Status = Job.InFlight; LastUpdated = now; StartedAt = Some now }))
                Task.FromResult(result)
            member this.SetDone job =
                lock lockObj (fun () ->
                    store <-
                        store
                        |> List.map (fun j ->
                            if j.Id = job.Id then
                                { j with Status = Job.Done; LastUpdated = DateTime.UtcNow; CompletedAt = job.CompletedAt }
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
                                { j with Status = Job.Failed; LastUpdated = DateTime.UtcNow; ErrorMessage = job.ErrorMessage }
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
            member this.SetRetry job runAfter =
                lock lockObj (fun () ->
                    store <-
                        store
                        |> List.map (fun j ->
                            if j.Id = job.Id then
                                { j with Status = Job.Waiting; RetryCount = j.RetryCount + 1; OnlyRunAfter = Some runAfter; LastUpdated = DateTime.UtcNow }
                            else j))
                Task.CompletedTask
            member this.RegisterWithDedup job dedupKey =
                let result = lock lockObj (fun () ->
                    let exists = store |> List.exists (fun j ->
                        j.DedupKey = Some dedupKey && (j.Status = Job.Waiting || j.Status = Job.InFlight))
                    if exists then false
                    else
                        let newJob = Job.createWithDedup job None dedupKey
                        store <- store @ [newJob]
                        true)
                Task.FromResult(result)
            member this.ScheduleWithDedup job shouldRunAfter dedupKey =
                let result = lock lockObj (fun () ->
                    let exists = store |> List.exists (fun j ->
                        j.DedupKey = Some dedupKey && (j.Status = Job.Waiting || j.Status = Job.InFlight))
                    if exists then false
                    else
                        let newJob = Job.createWithDedup job (Some shouldRunAfter) dedupKey
                        store <- store @ [newJob]
                        true)
                Task.FromResult(result)
            member this.ReclaimStale timeout =
                let result = lock lockObj (fun () ->
                    let cutoff = DateTime.UtcNow - timeout
                    let stale =
                        store |> List.filter (fun j ->
                            j.Status = Job.InFlight && j.StartedAt.IsSome && j.StartedAt.Value < cutoff)
                    store <-
                        store
                        |> List.map (fun j ->
                            if stale |> List.exists (fun s -> s.Id = j.Id) then
                                { j with Status = Job.Waiting; StartedAt = None; LastUpdated = DateTime.UtcNow }
                            else j)
                    stale.Length)
                Task.FromResult(result)

        interface IDashboardDataLayer with
            member this.QueryJobs query =
                let result = lock lockObj (fun () ->
                    let filtered =
                        match query.Status with
                        | Some s -> store |> List.filter (fun j -> j.Status = s)
                        | None -> store
                    let total = filtered.Length
                    let page =
                        filtered
                        |> List.sortByDescending (fun j -> j.LastUpdated)
                        |> List.skip (max 0 ((query.Page - 1) * query.PageSize))
                        |> List.truncate query.PageSize
                    (page, total))
                Task.FromResult(result)

            member this.GetJob id =
                let result = lock lockObj (fun () ->
                    store |> List.tryFind (fun j -> j.Id = id))
                Task.FromResult(result)

            member this.GetStats () =
                let result = lock lockObj (fun () ->
                    let now = DateTime.UtcNow
                    let byStatus s = store |> List.filter (fun j -> j.Status = s) |> List.length
                    let completed = store |> List.filter (fun j -> j.Status = Job.Done && j.CompletedAt.IsSome && j.StartedAt.IsSome)
                    let avgDuration =
                        match completed with
                        | [] -> None
                        | jobs ->
                            jobs
                            |> List.averageBy (fun j -> (j.CompletedAt.Value - j.StartedAt.Value).TotalSeconds)
                            |> Some
                    let lastHour =
                        store
                        |> List.filter (fun j ->
                            j.Status = Job.Done
                            && j.CompletedAt.IsSome
                            && j.CompletedAt.Value >= now.AddHours(-1.0))
                        |> List.length
                    { JobStats.WaitingCount = byStatus Job.Waiting
                      InFlightCount = byStatus Job.InFlight
                      DoneCount = byStatus Job.Done
                      FailedCount = byStatus Job.Failed
                      AverageDurationSeconds = avgDuration
                      JobsCompletedLastHour = lastHour })
                Task.FromResult(result)

            member this.RetryJob id =
                let result = lock lockObj (fun () ->
                    match store |> List.tryFind (fun j -> j.Id = id && j.Status = Job.Failed) with
                    | Some _ ->
                        store <-
                            store
                            |> List.map (fun j ->
                                if j.Id = id then
                                    { j with Status = Job.Waiting; ErrorMessage = None; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow }
                                else j)
                        true
                    | None -> false)
                Task.FromResult(result)

            member this.RequeueJob id =
                let result = lock lockObj (fun () ->
                    match store |> List.tryFind (fun j -> j.Id = id) with
                    | Some _ ->
                        store <-
                            store
                            |> List.map (fun j ->
                                if j.Id = id then
                                    { j with Status = Job.Waiting; ErrorMessage = None; OnlyRunAfter = None; StartedAt = None; CompletedAt = None; RetryCount = 0; LastUpdated = DateTime.UtcNow }
                                else j)
                        true
                    | None -> false)
                Task.FromResult(result)

            member this.DeleteJob id =
                let result = lock lockObj (fun () ->
                    let before = store.Length
                    store <- store |> List.filter (fun j -> j.Id <> id)
                    store.Length < before)
                Task.FromResult(result)

            member this.PurgeJobs status olderThan =
                let result = lock lockObj (fun () ->
                    let cutoff = DateTime.UtcNow - olderThan
                    let before = store.Length
                    store <- store |> List.filter (fun j ->
                        not (j.Status = status && j.LastUpdated < cutoff))
                    before - store.Length)
                Task.FromResult(result)

    let create<'t> () =
        InMemory<'t>() :> IDataLayer<'t>

    let createAsync<'t> () = task {
        let instance = InMemory<'t>()
        do! (instance :> IDataLayer<'t>).Setup()
        return instance :> IDataLayer<'t>
    }
