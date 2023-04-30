open System
open System.Threading
open Scheduler

type Person = { Name : string }

let add x y = x + y

let sendMailToPerson person =
    printfn $"hi %s{person.Name}"

type Foo =
    | Add of int * int
    | Hi of Person

let evaluate job =
    match job with
    | Add (x, y) ->
        Thread.Sleep(1000)
        printfn $"We added %d{x} and %d{y} and got %d{add x y}"
    | Hi p ->
        Thread.Sleep(1000)
        sendMailToPerson p

type Fakabase() =
    let mutable store: Job.Job list = []
    interface DataLayer.IDataLayer<Foo> with
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
                        { j with Status = Job.Done; LastUpdated = DateTime.Now }
                    else j)
        member this.SetInFlight job =
            store <-
                store
                |> List.map (fun j ->
                    if j.Id = job.Id then
                        { j with Status = Job.InFlight; LastUpdated = DateTime.Now }
                    else j)
        member this.SetFailed job =
            store <-
                store
                |> List.map (fun j ->
                    if j.Id = job.Id then
                        { j with Status = Job.Failed; LastUpdated = DateTime.Now }
                    else j)

        member this.RegisterSafe _ _ = failwith "todo"
        member this.RepeatSafe _ _ _ = failwith "todo"
        member this.ScheduleSafe _ _ _ = failwith "todo"

let fakabase = Fakabase() :> DataLayer.IDataLayer<Foo>

schedulerBuilder<Foo> () {
    with_datalayer fakabase
    with_polling_interval (TimeSpan.FromSeconds 1)
    with_polling_window DateTime.Now
    with_max_jobs 1
    with_evaluator evaluate
}

for i in 0 .. 10 do
    fakabase.Schedule (Hi { Name = "SHOULD NEVER PRINT" }) (Some (DateTime.Now.AddDays 7))
    let add = Add(0, i)
    if i > 0 && i % 2 = 0 then
        let hi = Hi { Name = $"Name: {i}" }
        fakabase.Register hi
    fakabase.Register add
    Thread.Sleep(2)

Console.ReadKey() |> ignore