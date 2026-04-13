open System
open System.Threading
open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Steve
open Steve.Dashboard

type Message = { Message: string }

type ExampleTask =
    | Add of int * int
    | Print of Message
    | Fail of string

let dataLayer = InMemory.create<ExampleTask>()

let evaluate (t: ExampleTask) (_ct: CancellationToken) : Task = task {
    match t with
    | Add (x, y) -> printfn $"Added %d{x} + %d{y} = %d{x + y}"
    | Print m -> printfn $"Message: %s{m.Message}"
    | Fail msg -> failwith msg
}

// Start the scheduler
let handle =
    schedulerBuilder<ExampleTask> () {
        with_datalayer dataLayer
        with_polling_interval (TimeSpan.FromSeconds 1.)
        with_max_jobs 4
        with_evaluator evaluate
        with_max_retries 2
    }

// Register some test jobs
for i in 1..5 do
    dataLayer.Register(Add(i, i * 10)).Wait()

dataLayer.Register(Print { Message = "Hello from Steve!" }).Wait()
dataLayer.Register(Fail "This job will fail").Wait()
dataLayer.Schedule (Print { Message = "Delayed 30s" }) (DateTime.UtcNow.AddSeconds 30.) |> fun t -> t.Wait()

// Start web host with dashboard
let builder = WebApplication.CreateBuilder()
let app = builder.Build()

app.MapSteveDashboard(dataLayer :?> Steve.DataLayer.IDashboardDataLayer, "/steve") |> ignore
app.MapGet("/", Func<string>(fun () -> "Steve dashboard at /steve")) |> ignore

printfn "Dashboard: http://localhost:5000/steve"
app.Run("http://localhost:5000")
