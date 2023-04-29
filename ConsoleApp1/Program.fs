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
        member this.Register foo shouldRunAfter type' =
            let newJob = Job.create foo type' shouldRunAfter
            store <- store @ [newJob]
        member this.Get(dateTime) =
            store
            |> List.filter (fun j ->
                if dateTime.IsNone || j.OnlyRunAfter.IsNone then
                    j.Status = Job.Waiting
                else
                    j.Status = Job.Waiting &&
                    (j.OnlyRunAfter.IsSome &&
                    dateTime.Value > j.OnlyRunAfter.Value)
            )
        member this.SetDone(job: Job.Job) =
            store <-
                store
                |> List.map (fun j ->
                    if j.Id = job.Id then
                        { j with Status = Job.Done; LastUpdated = DateTime.Now }
                    else j)
        member this.SetInFlight(job) =
            store <-
                store
                |> List.map (fun j ->
                    if j.Id = job.Id then
                        { j with Status = Job.InFlight; LastUpdated = DateTime.Now }
                    else j)
        member this.SetFailed(job: Job.Job) =
            store <-
                store
                |> List.map (fun j ->
                    if j.Id = job.Id then
                        { j with Status = Job.Failed; LastUpdated = DateTime.Now }
                    else j)


let fakabase = Fakabase() :> DataLayer.IDataLayer<Foo>

let scheduler = Mailbox.Scheduler<Foo> (fakabase, (Some DateTime.Now), 1, evaluate)

for i in 0 .. 10 do
    fakabase.Register (Hi { Name = "SHOULD NEVER PRINT" }) (Some (DateTime.Now.AddDays 7)) Job.Single
    let add = Add(0, i)
    if i > 0 && i % 2 = 0 then
        let hi = Hi { Name = $"Name: {i}" }
        fakabase.Register hi None Job.Single
    fakabase.Register add None Job.Single

Console.ReadKey() |> ignore