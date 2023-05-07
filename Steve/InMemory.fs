namespace DataLayer

open System

module InMemory =
    type InMemory<'t>() =
        let mutable store: Job.Job list = []
        interface DataLayer.IDataLayer<'t> with
            member this.Setup () = ()
            member this.Register foo =
                let newJob = Job.create foo None
                store <- store @ [newJob]
            member this.Schedule foo shouldRunAfter =
                let newJob = Job.create foo (Some shouldRunAfter)
                store <- store @ [newJob]
            member this.Poll () =
                store
                |> List.filter (fun j ->
                    j.Status = Job.Waiting ||
                        j.Status = Job.Waiting &&
                        (j.OnlyRunAfter.IsSome &&
                        DateTime.Now > j.OnlyRunAfter.Value)
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

            member this.RegisterSafe foo _ =
                let newJob = Job.create foo None
                store <- store @ [newJob]
            member this.ScheduleSafe foo shouldRunAfter _ =
                let newJob = Job.create foo (Some shouldRunAfter)
                store <- store @ [newJob]

    let create<'t> () =
        let datalayer = InMemory<'t>() :> DataLayer.IDataLayer<'t>
        datalayer.Setup()
        datalayer
