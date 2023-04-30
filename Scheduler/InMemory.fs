namespace DataLayer

module InMemory =
    type InMemory<'t>() =
        let mutable store: Job.Job list = []
        interface DataLayer.IDataLayer<'t> with
            member this.Setup() = ()
            member this.Register foo =
                let newJob = Job.create foo Job.Single None
                store <- store @ [newJob]
            member this.Schedule foo shouldRunAfter =
                let newJob = Job.create foo Job.Single shouldRunAfter
                store <- store @ [newJob]
            member this.Repeat foo interval =
                printf $"Interval needs to be used: %A{interval}"
                let newJob = Job.create foo Job.Recurring None
                store <- store @ [newJob]
            member this.Get dateTime =
                store
                |> List.filter (fun j ->
                    if dateTime.IsNone || j.OnlyRunAfter.IsNone then
                        j.Status = Job.Waiting
                    else
                        j.Status = Job.Waiting &&
                        (j.OnlyRunAfter.IsSome &&
                        dateTime.Value > j.OnlyRunAfter.Value)
                )
            member this.SetDone job =
                store <-
                    store
                    |> List.map (fun j ->
                        if j.Id = job.Id then
                            { j with Status = Job.Done; LastUpdated = System.DateTime.Now }
                        else j)
            member this.SetInFlight job =
                store <-
                    store
                    |> List.map (fun j ->
                        if j.Id = job.Id then
                            { j with Status = Job.InFlight; LastUpdated = System.DateTime.Now }
                        else j)
            member this.SetFailed job =
                store <-
                    store
                    |> List.map (fun j ->
                        if j.Id = job.Id then
                            { j with Status = Job.Failed; LastUpdated = System.DateTime.Now }
                        else j)

            member this.RegisterSafe _ _ = failwith "todo"
            member this.RepeatSafe _ _ _ = failwith "todo"
            member this.ScheduleSafe _ _ _ = failwith "todo"

    let create<'t> () =
        InMemory<'t>() :> DataLayer.IDataLayer<'t>
