open System
open System.Threading
open DataLayer.DataLayer
open Microsoft.Data.SqlClient

open Scheduler

type Message = { Message : string }

let add x y = x + y

let printMessage message =
    printfn $"Message: %s{message.Message}"

type ExampleReducer =
    | Add of int * int
    | Print of Message
    | Recurring of DateTime


let dataLayer = DataLayer.InMemory.create<ExampleReducer>()

//let connectionString = "Server=localhost,1433;User=sa;Password=<YourStrong!Passw0rd>;Database=arrangement-db;Persist Security Info=False;Encrypt=False"
//let dataLayer = DataLayer.MSSQL.create<ExampleReducer>(connectionString)

let evaluate (datalayer: IDataLayer<ExampleReducer>) reducer =
    match reducer with
    | Add (x, y) ->
        printfn $"We added %d{x} and %d{y} and got %d{add x y}"
    | Print m ->
        printMessage m
    | Recurring ran ->
        let in1Hour = ran.AddHours 1
        (datalayer.Schedule (Recurring in1Hour) in1Hour).Wait()
        printfn $"It is now: {DateTime.UtcNow}"

schedulerBuilder<ExampleReducer> () {
    with_datalayer dataLayer
    with_polling_interval (TimeSpan.FromSeconds 1.)
    //with_max_jobs 100
    with_evaluator (evaluate dataLayer)
}

// We can register a bunch of jobs to be done immediately
for i in 0 .. 10 do
    let add = Add(0, i)
    if i > 0 && i % 2 = 0 then
        let print = Print { Message = $"Number is: {i}" }
        (dataLayer.Register print).Wait()
    (dataLayer.Register add).Wait()

// We can also register jobs outside of a loop
(dataLayer.Register (Print { Message = "We can register whenever we want" })).Wait()

// We can schedule jobs to happen X amount of time from now
(dataLayer.Schedule (Print { Message = "Should print in 10 seconds" }) (DateTime.UtcNow.AddSeconds 10)).Wait()
(dataLayer.Schedule (Print { Message = "Should print in 7 days" }) (DateTime.UtcNow.AddDays 7)).Wait()
(dataLayer.Schedule (Print { Message = "Should Print in 5 minutes" }) (DateTime.UtcNow.AddMinutes 5)).Wait()

let currentJobs = (dataLayer.Poll()).Result
printfn $"\n--- Queued jobs: {currentJobs.Length} ---"
for job in currentJobs do
    let scheduledFor =
        match job.OnlyRunAfter with
        | Some t -> $"scheduled for {t}"
        | None -> "run ASAP"
    printfn $"  [{job.Status}] {job.Task} ({scheduledFor})"
printfn $"---\n"

// We can schedule jobs to happen at regular intervals. Like every hour
// See the reducer
let time = DateTime.UtcNow.AddMinutes 2
(dataLayer.Schedule (Recurring time) time).Wait()

(*let connection = new SqlConnection(connectionString)
connection.Open()

// We can also schedule within a transaction.
let t1 = connection.BeginTransaction()
(dataLayer.RegisterSafe (Print { Message = "This will not be added as the transaction is rolledback" }) t1).Wait()
t1.Rollback()

let t2 = connection.BeginTransaction()
(dataLayer.RegisterSafe (Print { Message = "This will be added as the transaction is committed" }) t2).Wait()
t2.Commit()
connection.Close()
*)

Console.ReadKey() |> ignore
