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

let InMemoryDataLayer = DataLayer.InMemory.create<Foo>()

schedulerBuilder<Foo> () {
    with_datalayer InMemoryDataLayer
    with_polling_interval (TimeSpan.FromSeconds 1)
    with_polling_window DateTime.Now
    with_max_jobs 1
    with_evaluator evaluate
}

for i in 0 .. 10 do
    InMemoryDataLayer.Schedule (Hi { Name = "SHOULD NEVER PRINT" }) (Some (DateTime.Now.AddDays 7))
    let add = Add(0, i)
    if i > 0 && i % 2 = 0 then
        let hi = Hi { Name = $"Name: {i}" }
        InMemoryDataLayer.Register hi
    InMemoryDataLayer.Register add
    Thread.Sleep(2)

Console.ReadKey() |> ignore