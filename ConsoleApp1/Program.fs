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

//
type Fakabase() =
    let mutable store: Job.Job list = []
    interface DataLayer.IDataLayer<Foo> with
        member this.Register(foo: Foo) =
            let newJob = Job.create foo
            store <- store @ [newJob]
        member this.Get(dateTime) =
            store
            |> List.filter (fun j -> j.Status = Job.Waiting)
        member this.Update(job: Job.Job) =
            store <-
                store
                |> List.map (fun j ->
                    if j.Id = job.Id then
                        { j with Status = Job.Done }
                    else j)
        member this.SetInFlight(job) =
            store <-
                store
                |> List.map (fun j ->
                    if j.Id = job.Id then
                        { j with Status = Job.InFlight }
                    else j)

let fakabase = Fakabase() :> DataLayer.IDataLayer<Foo>

let scheduler = Mailbox.Scheduler<Foo> (fakabase, 1, evaluate)

for i in 0 .. 10 do
    let add = Add(0, i)
    if i > 0 && i % 2 = 0 then
        let hi = Hi { Name = $"Name: {i}" }
        fakabase.Register hi
    fakabase.Register add

System.Console.ReadKey()