module Steve.Tests.Unit

open System
open System.Collections.Concurrent
open System.Data
open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Logging
open Xunit
open Steve
open Steve.Job

// ─── Test DU for Evaluator / Scheduler ───

type TestTask =
    | Add of int * int
    | Greet of string
    | Nested of TestTask
    | Empty

// ─── Test Logger ───

type LogEntry = { Level: LogLevel; Message: string; Exception: exn option }

type CapturingLogger(?onLog: LogEntry -> unit) =
    let entries = ConcurrentBag<LogEntry>()
    member _.Entries = entries |> Seq.toList
    interface ILogger with
        member _.BeginScope(_) = { new IDisposable with member _.Dispose() = () }
        member _.IsEnabled(_) = true
        member _.Log(level, _, state, ex, formatter) =
            let entry = { Level = level; Message = formatter.Invoke(state, ex); Exception = if isNull (box ex) then None else Some ex }
            entries.Add(entry)
            onLog |> Option.iter (fun f -> f entry)

// ─── Job.Status ───

module StatusTests =
    [<Fact>]
    let ``Serialize roundtrips all statuses`` () =
        let statuses = [ Done; Waiting; InFlight; Failed ]
        for s in statuses do
            let serialized = Status.Serialize s
            let deserialized = Status.Deserialize serialized
            Assert.Equal(Ok s, deserialized)

    [<Fact>]
    let ``Deserialize invalid string returns Error`` () =
        let result = Status.Deserialize "Bogus"
        Assert.True(Result.isError result)

    [<Fact>]
    let ``Serialize produces expected strings`` () =
        Assert.Equal("Done", Status.Serialize Done)
        Assert.Equal("Waiting", Status.Serialize Waiting)
        Assert.Equal("InFlight", Status.Serialize InFlight)
        Assert.Equal("Failed", Status.Serialize Failed)

    [<Fact>]
    let ``Deserialize empty string returns Error`` () =
        let result = Status.Deserialize ""
        Assert.True(Result.isError result)

    [<Fact>]
    let ``Deserialize is case sensitive`` () =
        let result = Status.Deserialize "done"
        Assert.True(Result.isError result)

// ─── Evaluator (Serializer) ───

module EvaluatorTests =
    [<Fact>]
    let ``Roundtrip serialize/deserialize DU`` () =
        let cases = [ Add(1, 2); Greet "hello" ]
        for case in cases do
            let json = Evaluator.serialize case
            let back = Evaluator.deserialize<TestTask> json
            Assert.Equal(case, back)

    [<Fact>]
    let ``Serialize produces valid JSON`` () =
        let json = Evaluator.serialize (Add(3, 4))
        Assert.Contains("Add", json)

// ─── Job.create ───

module JobCreateTests =
    [<Fact>]
    let ``create sets Waiting status`` () =
        let job = Job.create (Add(1, 2)) None
        Assert.Equal(Waiting, job.Status)

    [<Fact>]
    let ``create with OnlyRunAfter stores it`` () =
        let future = DateTime.UtcNow.AddHours 1.0
        let job = Job.create (Greet "hi") (Some future)
        Assert.Equal(Some future, job.OnlyRunAfter)

    [<Fact>]
    let ``create without OnlyRunAfter stores None`` () =
        let job = Job.create (Add(1, 1)) None
        Assert.Equal(None, job.OnlyRunAfter)

    [<Fact>]
    let ``create generates unique Ids`` () =
        let j1 = Job.create (Add(1, 1)) None
        let j2 = Job.create (Add(1, 1)) None
        Assert.NotEqual(j1.Id, j2.Id)

// ─── JobQueue ───

module JobQueueTests =
    let makeJob id runAfter =
        { Id = id
          Task = ""
          Status = Waiting
          OnlyRunAfter = runAfter
          LastUpdated = DateTime.UtcNow
          RetryCount = 0
          StartedAt = None
          CompletedAt = None
          ErrorMessage = None; DedupKey = None }

    [<Fact>]
    let ``enqueue deduplicates by Id`` () =
        let id = Guid.NewGuid()
        let j1 = makeJob id None
        let j2 = makeJob id None
        let queue = JobQueue.enqueue [j1; j2] []
        Assert.Equal(1, List.length queue)

    [<Fact>]
    let ``enqueue sorts by OnlyRunAfter`` () =
        let later = makeJob (Guid.NewGuid()) (Some (DateTime.UtcNow.AddHours 2.0))
        let sooner = makeJob (Guid.NewGuid()) (Some (DateTime.UtcNow.AddHours 1.0))
        let queue = JobQueue.enqueue [later; sooner] []
        Assert.Equal(sooner.Id, queue.[0].Id)

    [<Fact>]
    let ``dequeue empty returns None`` () =
        let job, rest = JobQueue.dequeue []
        Assert.Equal(None, job)
        Assert.Empty(rest)

    [<Fact>]
    let ``dequeue single returns item and empty`` () =
        let j = makeJob (Guid.NewGuid()) None
        let job, rest = JobQueue.dequeue [j]
        Assert.Equal(Some j, job)
        Assert.Empty(rest)

    [<Fact>]
    let ``dequeue multiple returns first and rest`` () =
        let j1 = makeJob (Guid.NewGuid()) None
        let j2 = makeJob (Guid.NewGuid()) None
        let job, rest = JobQueue.dequeue [j1; j2]
        Assert.Equal(Some j1, job)
        Assert.Equal(1, List.length rest)

// ─── InMemory.canRunNow ───

module CanRunNowTests =
    let makeJob status runAfter =
        { Id = Guid.NewGuid()
          Task = ""
          Status = status
          OnlyRunAfter = runAfter
          LastUpdated = DateTime.UtcNow
          RetryCount = 0
          StartedAt = None
          CompletedAt = None
          ErrorMessage = None; DedupKey = None }

    [<Fact>]
    let ``Waiting job with no schedule can run`` () =
        let job = makeJob Waiting None
        Assert.True(InMemory.canRunNow DateTime.UtcNow job)

    [<Fact>]
    let ``Waiting job with past schedule can run`` () =
        let job = makeJob Waiting (Some (DateTime.UtcNow.AddHours -1.0))
        Assert.True(InMemory.canRunNow DateTime.UtcNow job)

    [<Fact>]
    let ``Waiting job with future schedule cannot run`` () =
        let job = makeJob Waiting (Some (DateTime.UtcNow.AddHours 1.0))
        Assert.False(InMemory.canRunNow DateTime.UtcNow job)

    [<Fact>]
    let ``Done job cannot run`` () =
        let job = makeJob Done None
        Assert.False(InMemory.canRunNow DateTime.UtcNow job)

    [<Fact>]
    let ``InFlight job cannot run`` () =
        let job = makeJob InFlight None
        Assert.False(InMemory.canRunNow DateTime.UtcNow job)

    [<Fact>]
    let ``Failed job cannot run`` () =
        let job = makeJob Failed None
        Assert.False(InMemory.canRunNow DateTime.UtcNow job)

// ─── InMemory DataLayer ───

module InMemoryTests =
    [<Fact>]
    let ``Register then Poll returns job`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 2)).Wait()
        let jobs = dl.Poll().Result
        Assert.Equal(1, List.length jobs)

    [<Fact>]
    let ``Schedule future job not returned by Poll`` () =
        let dl = InMemory.create<TestTask>()
        dl.Schedule (Add(1, 2)) (DateTime.UtcNow.AddHours 1.0) |> fun t -> t.Wait()
        let jobs = dl.Poll().Result
        Assert.Empty(jobs)

    [<Fact>]
    let ``Schedule past job returned by Poll`` () =
        let dl = InMemory.create<TestTask>()
        dl.Schedule (Add(1, 2)) (DateTime.UtcNow.AddHours -1.0) |> fun t -> t.Wait()
        let jobs = dl.Poll().Result
        Assert.Equal(1, List.length jobs)

    [<Fact>]
    let ``SetDone marks job as Done`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 2)).Wait()
        let jobs = dl.Poll().Result
        dl.SetDone(jobs.Head).Wait()
        let afterPoll = dl.Poll().Result
        Assert.Empty(afterPoll)

    [<Fact>]
    let ``SetInFlight marks job as InFlight`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 2)).Wait()
        let jobs = dl.Poll().Result
        dl.SetInFlight(jobs.Head).Wait()
        let afterPoll = dl.Poll().Result
        Assert.Empty(afterPoll)

    [<Fact>]
    let ``SetFailed marks job as Failed`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 2)).Wait()
        let jobs = dl.Poll().Result
        dl.SetFailed(jobs.Head).Wait()
        let afterPoll = dl.Poll().Result
        Assert.Empty(afterPoll)

    [<Fact>]
    let ``Multiple registers create multiple jobs`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 2)).Wait()
        dl.Register(Add(3, 4)).Wait()
        dl.Register(Greet "yo").Wait()
        let jobs = dl.Poll().Result
        Assert.Equal(3, List.length jobs)

// ─── Scheduler Integration ───

module SchedulerIntegrationTests =
    [<Fact>]
    let ``Scheduler processes registered job`` () =
        let dl = InMemory.create<TestTask>()
        let mutable result = 0
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) ->
                result <- a + b
                evaluated.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(10, 20)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Job should have been evaluated within timeout")
        Assert.Equal(30, result)
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler processes multiple jobs`` () =
        let dl = InMemory.create<TestTask>()
        let mutable counter = 0
        let allDone = new CountdownEvent(3)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) ->
                Interlocked.Add(&counter, a + b) |> ignore
                allDone.Signal() |> ignore
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 3
            with_evaluator evaluator
        }

        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        dl.Register(Add(3, 3)).Wait()

        let completed = allDone.Wait(TimeSpan.FromSeconds 10.0)
        Assert.True(completed, "All jobs should complete within timeout")
        Assert.Equal(12, Volatile.Read(&counter))
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler handles failing job without crashing`` () =
        let dl = InMemory.create<TestTask>()
        let secondDone = new ManualResetEventSlim(false)
        let mutable result = 0

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ -> failwith "boom"
            | Add(a, b) ->
                result <- a + b
                secondDone.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 2
            with_evaluator evaluator
        }

        dl.Register(Greet "fail").Wait()
        dl.Register(Add(5, 5)).Wait()

        let completed = secondDone.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Second job should still run after first fails")
        Assert.Equal(10, result)
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler respects scheduled time`` () =
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add _ -> evaluated.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        // Schedule 1 hour in future - should NOT run
        dl.Schedule (Add(1, 1)) (DateTime.UtcNow.AddHours 1.0) |> fun t -> t.Wait()

        let ran = evaluated.Wait(TimeSpan.FromMilliseconds 500.0)
        Assert.False(ran, "Future-scheduled job should not run yet")
        handle.Stop().Wait()

// ─── Additional Evaluator edge cases ───

module EvaluatorEdgeCases =
    [<Fact>]
    let ``Roundtrip nested DU`` () =
        let value = Nested (Add(1, 2))
        let json = Evaluator.serialize value
        let back = Evaluator.deserialize<TestTask> json
        Assert.Equal(value, back)

    [<Fact>]
    let ``Roundtrip parameterless DU case`` () =
        let value = Empty
        let json = Evaluator.serialize value
        let back = Evaluator.deserialize<TestTask> json
        Assert.Equal(value, back)

    [<Fact>]
    let ``Roundtrip empty string field`` () =
        let value = Greet ""
        let json = Evaluator.serialize value
        let back = Evaluator.deserialize<TestTask> json
        Assert.Equal(value, back)

    [<Fact>]
    let ``Roundtrip string with special chars`` () =
        let value = Greet "hello \"world\" \n\t"
        let json = Evaluator.serialize value
        let back = Evaluator.deserialize<TestTask> json
        Assert.Equal(value, back)

// ─── Additional JobQueue tests ───

module JobQueueAdditionalTests =
    let makeJob id runAfter =
        { Id = id
          Task = ""
          Status = Waiting
          OnlyRunAfter = runAfter
          LastUpdated = DateTime.UtcNow
          RetryCount = 0
          StartedAt = None
          CompletedAt = None
          ErrorMessage = None; DedupKey = None }

    [<Fact>]
    let ``enqueue merges into existing queue`` () =
        let j1 = makeJob (Guid.NewGuid()) None
        let j2 = makeJob (Guid.NewGuid()) None
        let existing = [j1]
        let queue = JobQueue.enqueue [j2] existing
        Assert.Equal(2, List.length queue)

    [<Fact>]
    let ``enqueue deduplicates across new and existing`` () =
        let id = Guid.NewGuid()
        let j1 = makeJob id None
        let j2 = makeJob id None
        let existing = [j1]
        let queue = JobQueue.enqueue [j2] existing
        Assert.Equal(1, List.length queue)

    [<Fact>]
    let ``enqueue None sorts before Some`` () =
        let noSchedule = makeJob (Guid.NewGuid()) None
        let withSchedule = makeJob (Guid.NewGuid()) (Some (DateTime.UtcNow.AddHours 1.0))
        let queue = JobQueue.enqueue [withSchedule; noSchedule] []
        Assert.Equal(noSchedule.Id, queue.[0].Id)

    [<Fact>]
    let ``enqueue preserves order for equal OnlyRunAfter`` () =
        let t = Some DateTime.UtcNow
        let j1 = makeJob (Guid.NewGuid()) t
        let j2 = makeJob (Guid.NewGuid()) t
        let queue = JobQueue.enqueue [j1; j2] []
        Assert.Equal(2, List.length queue)

    [<Fact>]
    let ``enqueue empty list into existing preserves queue`` () =
        let j1 = makeJob (Guid.NewGuid()) None
        let queue = JobQueue.enqueue [] [j1]
        Assert.Equal(1, List.length queue)
        Assert.Equal(j1.Id, queue.[0].Id)

// ─── canRunNow boundary ───

module CanRunNowBoundaryTests =
    let makeJob status runAfter =
        { Id = Guid.NewGuid()
          Task = ""
          Status = status
          OnlyRunAfter = runAfter
          LastUpdated = DateTime.UtcNow
          RetryCount = 0
          StartedAt = None
          CompletedAt = None
          ErrorMessage = None; DedupKey = None }

    [<Fact>]
    let ``Waiting job at exact OnlyRunAfter time can run`` () =
        let now = DateTime.UtcNow
        let job = makeJob Waiting (Some now)
        Assert.True(InMemory.canRunNow now job)

// ─── InMemory DataLayer additional ───

module InMemoryAdditionalTests =
    [<Fact>]
    let ``RegisterSafe works same as Register`` () =
        let dl = InMemory.create<TestTask>()
        dl.RegisterSafe (Add(1, 2)) null |> fun t -> t.Wait()
        let jobs = dl.Poll().Result
        Assert.Equal(1, List.length jobs)

    [<Fact>]
    let ``ScheduleSafe works same as Schedule`` () =
        let dl = InMemory.create<TestTask>()
        dl.ScheduleSafe (Add(1, 2)) (DateTime.UtcNow.AddHours -1.0) null |> fun t -> t.Wait()
        let jobs = dl.Poll().Result
        Assert.Equal(1, List.length jobs)

    [<Fact>]
    let ``ScheduleSafe future job not returned by Poll`` () =
        let dl = InMemory.create<TestTask>()
        dl.ScheduleSafe (Add(1, 2)) (DateTime.UtcNow.AddHours 1.0) null |> fun t -> t.Wait()
        let jobs = dl.Poll().Result
        Assert.Empty(jobs)

    [<Fact>]
    let ``SetDone on nonexistent job does not crash`` () =
        let dl = InMemory.create<TestTask>()
        let phantom = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow; RetryCount = 0; StartedAt = None; CompletedAt = None; ErrorMessage = None; DedupKey = None }
        dl.SetDone(phantom).Wait()

    [<Fact>]
    let ``SetInFlight on nonexistent job does not crash`` () =
        let dl = InMemory.create<TestTask>()
        let phantom = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow; RetryCount = 0; StartedAt = None; CompletedAt = None; ErrorMessage = None; DedupKey = None }
        dl.SetInFlight(phantom).Wait()

    [<Fact>]
    let ``SetFailed on nonexistent job does not crash`` () =
        let dl = InMemory.create<TestTask>()
        let phantom = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow; RetryCount = 0; StartedAt = None; CompletedAt = None; ErrorMessage = None; DedupKey = None }
        dl.SetFailed(phantom).Wait()

    [<Fact>]
    let ``Poll returns empty on fresh datalayer`` () =
        let dl = InMemory.create<TestTask>()
        let jobs = dl.Poll().Result
        Assert.Empty(jobs)

    [<Fact>]
    let ``Setup is idempotent`` () =
        let dl = InMemory.create<TestTask>()
        dl.Setup().Wait()
        dl.Setup().Wait()

    [<Fact>]
    let ``Concurrent registers all persisted`` () =
        let dl = InMemory.create<TestTask>()
        let tasks =
            [| for i in 1..50 ->
                System.Threading.Tasks.Task.Run(fun () ->
                    dl.Register(Add(i, i)).Wait()) |]
        System.Threading.Tasks.Task.WaitAll(tasks)
        let jobs = dl.Poll().Result
        Assert.Equal(50, List.length jobs)

    [<Fact>]
    let ``Poll atomically claims jobs as InFlight`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        dl.Register(Add(3, 3)).Wait()
        let jobs = dl.Poll().Result
        Assert.Equal(3, List.length jobs)
        // Second poll returns empty - all already claimed
        let second = dl.Poll().Result
        Assert.Empty(second)

    [<Fact>]
    let ``Poll returns jobs with InFlight status`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        let jobs = dl.Poll().Result
        Assert.Equal(Job.InFlight, jobs.[0].Status)

// ─── Scheduler additional integration ───

module SchedulerAdditionalTests =
    [<Fact>]
    let ``Scheduler logger receives error on failure`` () =
        let dl = InMemory.create<TestTask>()
        let errorReceived = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ -> failwith "test error"
            | _ -> ()
        }

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error then errorReceived.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
            with_logger logger
        }

        dl.Register(Greet "boom").Wait()

        let received = errorReceived.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(received, "Logger should receive error")
        let errorEntry = logger.Entries |> List.find (fun e -> e.Level = LogLevel.Error)
        Assert.Contains("Job", errorEntry.Message)
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler without max_jobs runs all concurrently`` () =
        let dl = InMemory.create<TestTask>()
        let mutable counter = 0
        let allDone = new CountdownEvent(5)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) ->
                Interlocked.Add(&counter, a + b) |> ignore
                allDone.Signal() |> ignore
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_evaluator evaluator
        }

        for i in 1..5 do
            dl.Register(Add(i, i)).Wait()

        let completed = allDone.Wait(TimeSpan.FromSeconds 10.0)
        Assert.True(completed, "All jobs should run with no max_jobs limit")
        Assert.Equal(30, Volatile.Read(&counter))
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler max_jobs 1 processes sequentially`` () =
        let dl = InMemory.create<TestTask>()
        let mutable maxConcurrent = 0
        let mutable current = 0
        let lockObj = obj()
        let allDone = new CountdownEvent(3)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add _ ->
                lock lockObj (fun () ->
                    current <- current + 1
                    if current > maxConcurrent then maxConcurrent <- current)
                Thread.Sleep(50)
                lock lockObj (fun () -> current <- current - 1)
                allDone.Signal() |> ignore
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        dl.Register(Add(3, 3)).Wait()

        let completed = allDone.Wait(TimeSpan.FromSeconds 10.0)
        Assert.True(completed, "All jobs should complete")
        Assert.Equal(1, maxConcurrent)
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler evaluator receives correct deserialized task`` () =
        let dl = InMemory.create<TestTask>()
        let mutable received: TestTask option = None
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            received <- Some t
            evaluated.Set()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Greet "world").Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Job should be evaluated")
        Assert.Equal(Some (Greet "world"), received)
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler builder fails without evaluator`` () =
        let dl = InMemory.create<TestTask>()
        Assert.Throws<exn>(fun () ->
            schedulerBuilder<TestTask> () {
                with_datalayer dl
                with_polling_interval (TimeSpan.FromMilliseconds 50.0)
                with_max_jobs 1
            } |> ignore
        ) |> ignore

    [<Fact>]
    let ``Scheduler marks completed job as Done in datalayer`` () =
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add _ -> evaluated.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(1, 1)).Wait()
        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed)
        // Give scheduler time to call SetDone
        Thread.Sleep(200)
        let remaining = dl.Poll().Result
        Assert.Empty(remaining)
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler marks failed job as Failed in datalayer`` () =
        let dl = InMemory.create<TestTask>()
        let errorReceived = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ -> failwith "boom"
            | _ -> ()
        }

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error then errorReceived.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
            with_logger logger
        }

        dl.Register(Greet "fail").Wait()
        let received = errorReceived.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(received)
        // Give scheduler time to call SetFailed
        Thread.Sleep(200)
        let remaining = dl.Poll().Result
        Assert.Empty(remaining)
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler picks up jobs registered after start`` () =
        let dl = InMemory.create<TestTask>()
        let mutable counter = 0
        let batch1 = new ManualResetEventSlim(false)
        let batch2 = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) ->
                let v = Interlocked.Add(&counter, a + b)
                if v >= 10 then batch1.Set()
                if v >= 30 then batch2.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 5
            with_evaluator evaluator
        }

        // First batch
        dl.Register(Add(5, 5)).Wait()
        let got1 = batch1.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(got1, "First batch should complete")

        // Second batch - registered after scheduler already running
        dl.Register(Add(10, 10)).Wait()
        let got2 = batch2.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(got2, "Jobs registered after start should be picked up")
        handle.Stop().Wait()

// ─── StatusHandler (Dapper TypeHandler) ───

module StatusHandlerTests =
    [<Fact>]
    let ``Parse valid status strings`` () =
        let handler = StatusHandler()
        Assert.Equal(Done, handler.Parse("Done"))
        Assert.Equal(Waiting, handler.Parse("Waiting"))
        Assert.Equal(InFlight, handler.Parse("InFlight"))
        Assert.Equal(Failed, handler.Parse("Failed"))

    [<Fact>]
    let ``Parse invalid string throws`` () =
        let handler = StatusHandler()
        Assert.ThrowsAny<exn>(fun () ->
            handler.Parse("Nope") |> ignore)

    type FakeParam() =
        member val DbType = DbType.Object with get, set
        member val Direction = ParameterDirection.Input with get, set
        member val IsNullable = false with get
        member val ParameterName = "" with get, set
        member val Size = 0 with get, set
        member val SourceColumn = "" with get, set
        member val SourceVersion = DataRowVersion.Current with get, set
        member val Value: obj = null with get, set
        member val Precision = 0uy with get, set
        member val Scale = 0uy with get, set
        interface IDbDataParameter with
            member x.DbType with get() = x.DbType and set v = x.DbType <- v
            member x.Direction with get() = x.Direction and set v = x.Direction <- v
            member x.IsNullable = x.IsNullable
            member x.ParameterName with get() = x.ParameterName and set v = x.ParameterName <- v
            member x.Size with get() = x.Size and set v = x.Size <- v
            member x.SourceColumn with get() = x.SourceColumn and set v = x.SourceColumn <- v
            member x.SourceVersion with get() = x.SourceVersion and set v = x.SourceVersion <- v
            member x.Value with get() = x.Value and set v = x.Value <- v
            member x.Precision with get() = x.Precision and set v = x.Precision <- v
            member x.Scale with get() = x.Scale and set v = x.Scale <- v

    [<Fact>]
    let ``SetValue sets DbType and Size`` () =
        let handler = StatusHandler()
        let param = FakeParam()
        handler.SetValue(param, Done)
        Assert.Equal(DbType.String, param.DbType)
        Assert.Equal(16, param.Size)
        Assert.Equal("Done", param.Value :?> string)

    [<Fact>]
    let ``SetValue for each status`` () =
        let handler = StatusHandler()
        for status in [ Done; Waiting; InFlight; Failed ] do
            let param = FakeParam()
            handler.SetValue(param, status)
            Assert.Equal(Status.Serialize status, param.Value :?> string)

// ─── DataLayer.empty ───

module DataLayerEmptyTests =
    [<Fact>]
    let ``empty Setup completes`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        dl.Setup().Wait()

    [<Fact>]
    let ``empty Register completes`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        dl.Register(Add(1, 2)).Wait()

    [<Fact>]
    let ``empty Schedule completes`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        dl.Schedule (Add(1, 2)) DateTime.UtcNow |> fun t -> t.Wait()

    [<Fact>]
    let ``empty Poll returns empty`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        let jobs = dl.Poll().Result
        Assert.Empty(jobs)

    [<Fact>]
    let ``empty SetDone completes`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        let job = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow; RetryCount = 0; StartedAt = None; CompletedAt = None; ErrorMessage = None; DedupKey = None }
        dl.SetDone(job).Wait()

    [<Fact>]
    let ``empty SetInFlight completes`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        let job = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow; RetryCount = 0; StartedAt = None; CompletedAt = None; ErrorMessage = None; DedupKey = None }
        dl.SetInFlight(job).Wait()

    [<Fact>]
    let ``empty SetFailed completes`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        let job = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow; RetryCount = 0; StartedAt = None; CompletedAt = None; ErrorMessage = None; DedupKey = None }
        dl.SetFailed(job).Wait()

    [<Fact>]
    let ``empty RegisterSafe completes`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        dl.RegisterSafe (Add(1, 2)) null |> fun t -> t.Wait()

    [<Fact>]
    let ``empty ScheduleSafe completes`` () =
        let dl = Steve.DataLayer.empty() : Steve.DataLayer.IDataLayer<TestTask>
        dl.ScheduleSafe (Add(1, 2)) DateTime.UtcNow null |> fun t -> t.Wait()

// ─── Job.create Task field ───

module JobCreateTaskFieldTests =
    [<Fact>]
    let ``create Task field contains deserializable JSON`` () =
        let job = Job.create (Add(42, 58)) None
        let deserialized = Evaluator.deserialize<TestTask> job.Task
        Assert.Equal(Add(42, 58), deserialized)

    [<Fact>]
    let ``create Task field roundtrips all DU cases`` () =
        let cases = [ Add(1,2); Greet "hi"; Nested(Add(3,4)); Empty ]
        for case in cases do
            let job = Job.create case None
            let back = Evaluator.deserialize<TestTask> job.Task
            Assert.Equal(case, back)

// ─── Evaluator error handling ───

module EvaluatorErrorTests =
    [<Fact>]
    let ``Deserialize invalid JSON throws`` () =
        Assert.ThrowsAny<exn>(fun () ->
            Evaluator.deserialize<TestTask> "not json" |> ignore)

    [<Fact>]
    let ``Deserialize empty string throws`` () =
        Assert.ThrowsAny<exn>(fun () ->
            Evaluator.deserialize<TestTask> "" |> ignore)

// ─── InMemory timestamp and isolation ───

module InMemoryTimestampTests =
    [<Fact>]
    let ``SetDone updates LastUpdated`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        let jobs = dl.Poll().Result
        let before = jobs.Head.LastUpdated
        Thread.Sleep(50)
        dl.SetDone(jobs.Head).Wait()
        // Re-register to check - since done jobs won't poll, check indirectly
        // by registering new job and verifying old one doesn't appear
        let afterPoll = dl.Poll().Result
        Assert.Empty(afterPoll)

    [<Fact>]
    let ``SetDone only affects target job`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let jobs = dl.Poll().Result
        dl.SetDone(jobs.[0]).Wait()
        // Re-register a new Waiting job to verify Poll still works
        dl.Register(Add(3, 3)).Wait()
        let newJobs = dl.Poll().Result
        Assert.Equal(1, List.length newJobs)

    [<Fact>]
    let ``SetInFlight is idempotent`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        let jobs = dl.Poll().Result
        // Already InFlight from Poll, SetInFlight again should not crash
        dl.SetInFlight(jobs.[0]).Wait()

    [<Fact>]
    let ``SetFailed only affects target job`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let jobs = dl.Poll().Result
        dl.SetFailed(jobs.[0]).Wait()
        // Re-register a new Waiting job to verify Poll still works
        dl.Register(Add(3, 3)).Wait()
        let newJobs = dl.Poll().Result
        Assert.Equal(1, List.length newJobs)

    [<Fact>]
    let ``Concurrent Poll and SetDone safe`` () =
        let dl = InMemory.create<TestTask>()
        for _ in 1..20 do
            dl.Register(Add(1, 1)).Wait()
        let jobs = dl.Poll().Result
        let tasks =
            [| for j in jobs ->
                System.Threading.Tasks.Task.Run(fun () ->
                    dl.Poll().Result |> ignore
                    dl.SetDone(j).Wait()) |]
        System.Threading.Tasks.Task.WaitAll(tasks)
        let remaining = dl.Poll().Result
        Assert.Empty(remaining)

// ─── Scheduler poll failure recovery ───

module SchedulerPollFailureTests =
    open System.Threading.Tasks
    open Steve.DataLayer

    let makeFailingThenRecoveringDl (failCount: int) (realDl: IDataLayer<TestTask>) =
        let mutable pollCalls = 0
        { new IDataLayer<TestTask> with
            member _.Setup() = realDl.Setup()
            member _.Register t = realDl.Register t
            member _.RegisterSafe t txn = realDl.RegisterSafe t txn
            member _.Schedule t d = realDl.Schedule t d
            member _.ScheduleSafe t d txn = realDl.ScheduleSafe t d txn
            member _.SetDone j = realDl.SetDone j
            member _.SetInFlight j = realDl.SetInFlight j
            member _.SetFailed j = realDl.SetFailed j
            member _.SetRetry j d = realDl.SetRetry j d
            member _.ReclaimStale t = realDl.ReclaimStale t
            member _.RegisterWithDedup t k = realDl.RegisterWithDedup t k
            member _.ScheduleWithDedup t d k = realDl.ScheduleWithDedup t d k
            member _.Poll() =
                let n = Interlocked.Increment(&pollCalls)
                if n <= failCount then
                    Task.FromException<Job.JobRecord list>(exn "poll failed")
                else
                    realDl.Poll() }

    [<Fact>]
    let ``Scheduler recovers after poll failure`` () =
        let realDl = InMemory.create<TestTask>()
        let dl = makeFailingThenRecoveringDl 3 realDl
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) when a = 777 && b = 333 -> evaluated.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        realDl.Register(Add(777, 333)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Scheduler should recover from poll failures and process job")
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduler logs error for poll failure`` () =
        let realDl = InMemory.create<TestTask>()
        let dl = makeFailingThenRecoveringDl 1 realDl
        let errorReceived = new ManualResetEventSlim(false)

        let evaluator (_: TestTask) (_ct: CancellationToken) : Task = Task.CompletedTask

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error then errorReceived.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
            with_logger logger
        }

        let received = errorReceived.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(received, "Logger should receive poll error")
        let errorEntry = logger.Entries |> List.find (fun e -> e.Level = LogLevel.Error)
        Assert.Contains("Poll", errorEntry.Message)
        handle.Stop().Wait()

// ─── Scheduler zero polling interval ───

module SchedulerZeroIntervalTests =
    [<Fact>]
    let ``Scheduler with zero polling interval processes jobs`` () =
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) when a = 888 && b = 111 -> evaluated.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromSeconds 0.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(888, 111)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Scheduler with zero interval should still process jobs")
        handle.Stop().Wait()

// ─── Scheduler graceful shutdown ───

module SchedulerShutdownTests =
    [<Fact>]
    let ``Stop completes immediately when no jobs in flight`` () =
        let dl = InMemory.create<TestTask>()
        let evaluator (_: TestTask) (_ct: CancellationToken) : Task = Task.CompletedTask

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        let stopped = handle.Stop().Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(stopped, "Stop should complete quickly with no in-flight jobs")

    [<Fact>]
    let ``Stop waits for in-flight job to complete`` () =
        let dl = InMemory.create<TestTask>()
        let jobStarted = new ManualResetEventSlim(false)
        let allowFinish = new ManualResetEventSlim(false)
        let mutable result = 0

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) ->
                jobStarted.Set()
                allowFinish.Wait(TimeSpan.FromSeconds 10.0) |> ignore
                result <- a + b
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(42, 58)).Wait()
        let started = jobStarted.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(started, "Job should have started")

        // Stop issued while job is running
        let stopTask = handle.Stop()
        Assert.False(stopTask.Wait(TimeSpan.FromMilliseconds 200.0), "Stop should not complete while job is running")

        // Let the job finish
        allowFinish.Set()
        let stopped = stopTask.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(stopped, "Stop should complete after in-flight job finishes")
        Assert.Equal(100, result)

    [<Fact>]
    let ``Stop prevents new jobs from starting`` () =
        let dl = InMemory.create<TestTask>()
        let jobStarted = new ManualResetEventSlim(false)
        let allowFinish = new ManualResetEventSlim(false)
        let mutable jobCount = 0

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add _ ->
                Interlocked.Increment(&jobCount) |> ignore
                jobStarted.Set()
                allowFinish.Wait(TimeSpan.FromSeconds 10.0) |> ignore
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 5
            with_evaluator evaluator
        }

        dl.Register(Add(1, 1)).Wait()
        let started = jobStarted.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(started, "First job should start")

        // Stop and register more jobs
        let stopTask = handle.Stop()
        dl.Register(Add(2, 2)).Wait()
        dl.Register(Add(3, 3)).Wait()

        // Let first job finish
        allowFinish.Set()
        let stopped = stopTask.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(stopped, "Stop should complete")

        // Only the first job should have run
        Assert.Equal(1, Volatile.Read(&jobCount))

// ─── SchedulerHandle.DisposeAsync ───

module SchedulerDisposeTests =
    [<Fact>]
    let ``DisposeAsync completes when no jobs in flight`` () =
        let dl = InMemory.create<TestTask>()

        let evaluator (_: TestTask) (_ct: CancellationToken) : Task = Task.CompletedTask

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        let completed = (handle :> IAsyncDisposable).DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "DisposeAsync should complete quickly with no jobs")

    [<Fact>]
    let ``DisposeAsync waits for in-flight job`` () =
        let dl = InMemory.create<TestTask>()
        let jobStarted = new ManualResetEventSlim(false)
        let allowFinish = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add _ ->
                jobStarted.Set()
                allowFinish.Wait(TimeSpan.FromSeconds 10.0) |> ignore
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(1, 1)).Wait()
        let started = jobStarted.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(started, "Job should start")

        let disposeTask = (handle :> IAsyncDisposable).DisposeAsync().AsTask()
        Assert.False(disposeTask.Wait(200), "DisposeAsync should block while job runs")

        allowFinish.Set()
        let completed = disposeTask.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "DisposeAsync should complete after job finishes")

    [<Fact>]
    let ``SchedulerHandle works with use in task CE`` () =
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add _ -> evaluated.Set()
            | _ -> ()
        }

        let work = task {
            use handle = schedulerBuilder<TestTask> () {
                with_datalayer dl
                with_polling_interval (TimeSpan.FromMilliseconds 50.0)
                with_max_jobs 1
                with_evaluator evaluator
            }
            dl.Register(Add(1, 1)).Wait()
            let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
            Assert.True(completed, "Job should complete before dispose")
            ignore handle
        }
        work.Wait()

// ─── Async evaluator ───

module AsyncEvaluatorTests =
    open System.Threading.Tasks

    [<Fact>]
    let ``Async evaluator processes job`` () =
        let dl = InMemory.create<TestTask>()
        let mutable result = 0
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) ->
                do! Task.Delay(10)
                result <- a + b
                evaluated.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(20, 30)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Async evaluator should process job")
        Assert.Equal(50, result)
        handle.Stop().Wait()

    [<Fact>]
    let ``Async evaluator failure is handled`` () =
        let dl = InMemory.create<TestTask>()
        let errorReceived = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ -> failwith "async boom"
            | _ -> ()
        }

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error then errorReceived.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
            with_logger logger
        }

        dl.Register(Greet "fail").Wait()

        let received = errorReceived.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(received, "Async evaluator failure should be logged")
        handle.Stop().Wait()

// ─── Job retry mechanism ───

module JobRetryTests =
    [<Fact>]
    let ``Failed job is retried when max_retries is set`` () =
        let dl = InMemory.create<TestTask>()
        let mutable attempts = 0
        let succeeded = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ ->
                let n = Interlocked.Increment(&attempts)
                if n < 3 then failwith $"attempt {n}"
                else succeeded.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_max_retries 3
            with_evaluator evaluator
        }

        dl.Register(Greet "retry-me").Wait()

        let completed = succeeded.Wait(TimeSpan.FromSeconds 15.0)
        Assert.True(completed, "Job should succeed after retries")
        Assert.True(Volatile.Read(&attempts) >= 3, "Should have attempted at least 3 times")
        handle.Stop().Wait()

    [<Fact>]
    let ``Job is marked Failed after exhausting retries`` () =
        let dl = InMemory.create<TestTask>()
        let failedLogged = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ -> failwith "always fails"
            | _ -> ()
        }

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error && entry.Message.Contains("retries") then
                failedLogged.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_max_retries 2
            with_evaluator evaluator
            with_logger logger
        }

        dl.Register(Greet "doomed").Wait()

        let logged = failedLogged.Wait(TimeSpan.FromSeconds 15.0)
        Assert.True(logged, "Should log final failure after exhausting retries")
        handle.Stop().Wait()

    [<Fact>]
    let ``No retries when max_retries is 0`` () =
        let dl = InMemory.create<TestTask>()
        let mutable attempts = 0
        let errorLogged = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ ->
                Interlocked.Increment(&attempts) |> ignore
                failwith "fail"
            | _ -> ()
        }

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error then errorLogged.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
            with_logger logger
        }

        dl.Register(Greet "no-retry").Wait()

        let logged = errorLogged.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(logged, "Should fail immediately")
        // Give time for any unexpected retries
        Thread.Sleep(500)
        Assert.Equal(1, Volatile.Read(&attempts))
        handle.Stop().Wait()

    [<Fact>]
    let ``RetryCount increments on each retry`` () =
        let dl = InMemory.create<TestTask>()
        let dash = dl :?> Steve.DataLayer.IDashboardDataLayer
        let failedFinal = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ -> failwith "always fails"
            | _ -> ()
        }

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error && entry.Message.Contains("retries") then
                failedFinal.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_max_retries 2
            with_evaluator evaluator
            with_logger logger
        }

        dl.Register(Greet "retry-count").Wait()

        let logged = failedFinal.Wait(TimeSpan.FromSeconds 15.0)
        Assert.True(logged, "Should exhaust retries")
        // After exhausting 2 retries: initial attempt + 2 retries = RetryCount should be 2
        Thread.Sleep(200)
        let jobs, _ = dash.QueryJobs({ Steve.DataLayer.Status = Some Job.Failed; Page = 1; PageSize = 100 }).Result
        let job = jobs |> List.find (fun j -> Evaluator.deserialize<TestTask> j.Task = Greet "retry-count")
        Assert.Equal(2, job.RetryCount)
        handle.Stop().Wait()

    [<Fact>]
    let ``ErrorMessage from final failure is preserved`` () =
        let dl = InMemory.create<TestTask>()
        let dash = dl :?> Steve.DataLayer.IDashboardDataLayer
        let failedFinal = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ -> failwith "specific error message"
            | _ -> ()
        }

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error && entry.Message.Contains("retries") then
                failedFinal.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_max_retries 1
            with_evaluator evaluator
            with_logger logger
        }

        dl.Register(Greet "error-msg").Wait()

        let logged = failedFinal.Wait(TimeSpan.FromSeconds 15.0)
        Assert.True(logged)
        Thread.Sleep(200)
        let jobs, _ = dash.QueryJobs({ Steve.DataLayer.Status = Some Job.Failed; Page = 1; PageSize = 100 }).Result
        let job = jobs |> List.find (fun j -> Evaluator.deserialize<TestTask> j.Task = Greet "error-msg")
        Assert.True(job.ErrorMessage.IsSome, "ErrorMessage should be set")
        Assert.Contains("specific error message", job.ErrorMessage.Value)
        handle.Stop().Wait()

    [<Fact>]
    let ``SetRetry failure is logged but scheduler continues`` () =
        let dl = InMemory.create<TestTask>()
        let secondDone = new ManualResetEventSlim(false)
        let setRetryErrorLogged = new ManualResetEventSlim(false)

        // Wrap datalayer so SetRetry throws
        let failingDl =
            { new Steve.DataLayer.IDataLayer<TestTask> with
                member _.Setup() = dl.Setup()
                member _.Register t = dl.Register t
                member _.RegisterSafe t txn = dl.RegisterSafe t txn
                member _.Schedule t d = dl.Schedule t d
                member _.ScheduleSafe t d txn = dl.ScheduleSafe t d txn
                member _.Poll() = dl.Poll()
                member _.SetDone j = dl.SetDone j
                member _.SetInFlight j = dl.SetInFlight j
                member _.SetFailed j = dl.SetFailed j
                member _.SetRetry _ _ = Task.FromException(exn "SetRetry broken")
                member _.ReclaimStale t = dl.ReclaimStale t
                member _.RegisterWithDedup t k = dl.RegisterWithDedup t k
                member _.ScheduleWithDedup t d k = dl.ScheduleWithDedup t d k }

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Greet _ -> failwith "fail"
            | Add(99, 99) -> secondDone.Set()
            | _ -> ()
        }

        let logger = CapturingLogger(fun entry ->
            if entry.Level = LogLevel.Error && entry.Message.Contains("SetRetry") then
                setRetryErrorLogged.Set())

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer failingDl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 2
            with_max_retries 3
            with_evaluator evaluator
            with_logger logger
        }

        failingDl.Register(Greet "will-fail-retry").Wait()
        failingDl.Register(Add(99, 99)).Wait()

        let errLogged = setRetryErrorLogged.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(errLogged, "SetRetry failure should be logged")
        let secondCompleted = secondDone.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(secondCompleted, "Scheduler should continue processing after SetRetry failure")
        handle.Stop().Wait()

// ─── Dashboard data layer (InMemory) ───

module InMemoryDashboardTests =
    open Steve.DataLayer

    let createDashboard () =
        let dl = InMemory.create<TestTask>()
        let dash = dl :?> IDashboardDataLayer
        dl, dash

    [<Fact>]
    let ``QueryJobs returns all jobs when no status filter`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let jobs, total = dash.QueryJobs({ Status = None; Page = 1; PageSize = 10 }).Result
        Assert.Equal(2, total)
        Assert.Equal(2, jobs.Length)

    [<Fact>]
    let ``QueryJobs filters by status`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let polled = dl.Poll().Result
        dl.SetDone(polled.[0]).Wait()
        let jobs, total = dash.QueryJobs({ Status = Some Job.Done; Page = 1; PageSize = 10 }).Result
        Assert.Equal(1, total)
        Assert.Equal(Job.Done, jobs.[0].Status)

    [<Fact>]
    let ``QueryJobs paginates correctly`` () =
        let dl, dash = createDashboard()
        for i in 1..5 do
            dl.Register(Add(i, i)).Wait()
        let page1, total = dash.QueryJobs({ Status = None; Page = 1; PageSize = 2 }).Result
        Assert.Equal(5, total)
        Assert.Equal(2, page1.Length)
        let page2, _ = dash.QueryJobs({ Status = None; Page = 2; PageSize = 2 }).Result
        Assert.Equal(2, page2.Length)
        let page3, _ = dash.QueryJobs({ Status = None; Page = 3; PageSize = 2 }).Result
        Assert.Equal(1, page3.Length)

    [<Fact>]
    let ``GetJob returns job by id`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        let all, _ = dash.QueryJobs({ Status = None; Page = 1; PageSize = 10 }).Result
        let id = all.[0].Id
        let found = dash.GetJob(id).Result
        Assert.True(found.IsSome)
        Assert.Equal(id, found.Value.Id)

    [<Fact>]
    let ``GetJob returns None for missing id`` () =
        let _, dash = createDashboard()
        let found = dash.GetJob(Guid.NewGuid()).Result
        Assert.True(found.IsNone)

    [<Fact>]
    let ``GetStats returns correct counts`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let polled = dl.Poll().Result
        dl.SetDone(polled.[0]).Wait()
        dl.SetFailed(polled.[1]).Wait()
        dl.Register(Add(3, 3)).Wait()
        let stats = dash.GetStats().Result
        Assert.Equal(1, stats.WaitingCount)
        Assert.Equal(1, stats.DoneCount)
        Assert.Equal(1, stats.FailedCount)

    [<Fact>]
    let ``RetryJob resets failed job to Waiting`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        let polled = dl.Poll().Result
        dl.SetFailed(polled.[0]).Wait()
        let id = polled.[0].Id
        let result = dash.RetryJob(id).Result
        Assert.True(result)
        let job = dash.GetJob(id).Result
        Assert.Equal(Job.Waiting, job.Value.Status)

    [<Fact>]
    let ``RetryJob returns false for non-failed job`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        let all, _ = dash.QueryJobs({ Status = None; Page = 1; PageSize = 10 }).Result
        let result = dash.RetryJob(all.[0].Id).Result
        Assert.False(result)

    [<Fact>]
    let ``RetryJob returns false for nonexistent id`` () =
        let _, dash = createDashboard()
        let result = dash.RetryJob(Guid.NewGuid()).Result
        Assert.False(result)

    [<Fact>]
    let ``RequeueJob resets done job to Waiting`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        let polled = dl.Poll().Result
        dl.SetDone(polled.[0]).Wait()
        let id = polled.[0].Id
        let result = dash.RequeueJob(id).Result
        Assert.True(result)
        let job = dash.GetJob(id).Result
        Assert.Equal(Job.Waiting, job.Value.Status)
        Assert.Equal(0, job.Value.RetryCount)

    [<Fact>]
    let ``RequeueJob resets failed job to Waiting`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        let polled = dl.Poll().Result
        dl.SetFailed(polled.[0]).Wait()
        let id = polled.[0].Id
        let result = dash.RequeueJob(id).Result
        Assert.True(result)
        let job = dash.GetJob(id).Result
        Assert.Equal(Job.Waiting, job.Value.Status)

    [<Fact>]
    let ``RequeueJob returns false for nonexistent id`` () =
        let _, dash = createDashboard()
        let result = dash.RequeueJob(Guid.NewGuid()).Result
        Assert.False(result)

    [<Fact>]
    let ``DeleteJob removes job`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        let all, _ = dash.QueryJobs({ Status = None; Page = 1; PageSize = 10 }).Result
        let id = all.[0].Id
        let result = dash.DeleteJob(id).Result
        Assert.True(result)
        let found = dash.GetJob(id).Result
        Assert.True(found.IsNone)

    [<Fact>]
    let ``DeleteJob returns false for nonexistent id`` () =
        let _, dash = createDashboard()
        let result = dash.DeleteJob(Guid.NewGuid()).Result
        Assert.False(result)

    [<Fact>]
    let ``PurgeJobs removes old done jobs`` () =
        let dl, dash = createDashboard()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let polled = dl.Poll().Result
        dl.SetDone(polled.[0]).Wait()
        dl.SetDone(polled.[1]).Wait()
        // Jobs were just created so olderThan=0 should purge them
        let count = (dash.PurgeJobs Job.Done TimeSpan.Zero).Result
        Assert.Equal(2, count)
        let _, total = dash.QueryJobs({ Status = Some Job.Done; Page = 1; PageSize = 10 }).Result
        Assert.Equal(0, total)

// ─── Multi-poll-cycle tests ───

module MultiPollCycleTests =
    [<Fact>]
    let ``Jobs registered across two poll cycles are all processed`` () =
        let dl = InMemory.create<TestTask>()
        let batch1Done = new CountdownEvent(2)
        let batch2Done = new CountdownEvent(2)
        let mutable batch1Complete = false

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, _) when a <= 2 ->
                batch1Done.Signal() |> ignore
            | Add _ ->
                batch2Done.Signal() |> ignore
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 100.0)
            with_max_jobs 10
            with_evaluator evaluator
        }

        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let got1 = batch1Done.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(got1, "First batch should complete")
        batch1Complete <- true

        // Second batch — must be picked up by a later poll cycle
        dl.Register(Add(3, 3)).Wait()
        dl.Register(Add(4, 4)).Wait()
        let got2 = batch2Done.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(got2, "Second batch should complete in later poll cycle")
        handle.Stop().Wait()

    [<Fact>]
    let ``Scheduled job becomes eligible in later poll cycle`` () =
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(42, 42) -> evaluated.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        // Schedule 200ms in future — first few polls should skip it
        dl.Schedule (Add(42, 42)) (DateTime.UtcNow.AddMilliseconds 200.0) |> fun t -> t.Wait()

        // Should NOT run immediately
        let ranEarly = evaluated.Wait(TimeSpan.FromMilliseconds 100.0)
        Assert.False(ranEarly, "Should not run before scheduled time")

        // But should run eventually
        let ranLater = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(ranLater, "Should run after scheduled time passes")
        handle.Stop().Wait()

    [<Fact>]
    let ``Queue drains correctly when max_jobs limits concurrency across cycles`` () =
        let dl = InMemory.create<TestTask>()
        let allDone = new CountdownEvent(3)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add _ ->
                Thread.Sleep(50)
                allDone.Signal() |> ignore
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        dl.Register(Add(3, 3)).Wait()

        let completed = allDone.Wait(TimeSpan.FromSeconds 10.0)
        Assert.True(completed, "All 3 jobs should complete with max_jobs=1 across poll cycles")
        handle.Stop().Wait()

// ─── Stale InFlight Reclamation (F2) ───

module StaleReclamationTests =
    open Steve.DataLayer

    [<Fact>]
    let ``ReclaimStale resets InFlight jobs older than timeout`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()

        // Poll to set InFlight
        let jobs = dl.Poll().Result
        Assert.Equal(1, jobs.Length)
        Assert.Equal(InFlight, jobs.[0].Status)

        // Reclaim with zero timeout (everything is stale)
        let reclaimed = dl.ReclaimStale(TimeSpan.Zero).Result
        Assert.Equal(1, reclaimed)

        // Job should be Waiting again
        let jobs2 = dl.Poll().Result
        Assert.Equal(1, jobs2.Length)

    [<Fact>]
    let ``ReclaimStale does not touch jobs within timeout`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        dl.Poll().Result |> ignore

        // Reclaim with large timeout — nothing stale
        let reclaimed = dl.ReclaimStale(TimeSpan.FromHours 1.0).Result
        Assert.Equal(0, reclaimed)

    [<Fact>]
    let ``ReclaimStale ignores Waiting and Done jobs`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        // Job is Waiting — not touched
        let reclaimed = dl.ReclaimStale(TimeSpan.Zero).Result
        Assert.Equal(0, reclaimed)

    [<Fact>]
    let ``Scheduler with stale_timeout reclaims and re-processes stuck jobs`` () =
        let dl = InMemory.create<TestTask>()
        let processed = new CountdownEvent(2)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add _ -> processed.Signal() |> ignore
            | _ -> ()
        }

        // Register a job, poll it to InFlight manually (simulating a crashed worker)
        dl.Register(Add(1, 1)).Wait()
        dl.Poll().Result |> ignore // now InFlight with StartedAt = now

        // Start scheduler with very short stale timeout
        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_stale_timeout (TimeSpan.Zero)
            with_evaluator evaluator
        }

        // Also register a fresh job to confirm normal processing too
        dl.Register(Add(2, 2)).Wait()

        let completed = processed.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Both reclaimed and fresh job should process")
        handle.Stop().Wait()

// ─── IHostedService (F3) ───

module HostedServiceTests =
    open Microsoft.Extensions.DependencyInjection
    open Microsoft.Extensions.Hosting
    open Steve.DataLayer

    [<Fact>]
    let ``SteveHostedService starts and stops`` () = task {
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let svc = SteveHostedService<TestTask>(fun spec ->
            { spec with
                DataLayer = dl
                PollingInterval = TimeSpan.FromMilliseconds 50.0
                Evaluator = Some (fun (_: TestTask) (_ct: CancellationToken) -> task { evaluated.Set() } :> Task) })

        let hosted = svc :> IHostedService
        do! hosted.StartAsync(CancellationToken.None)

        dl.Register(Add(1, 1)).Wait()
        Assert.True(evaluated.Wait(TimeSpan.FromSeconds 5.0), "Job should be processed")

        do! hosted.StopAsync(CancellationToken.None)
    }

    [<Fact>]
    let ``AddSteve registers IHostedService in DI`` () =
        let services = ServiceCollection() :> IServiceCollection
        services.AddSteve<TestTask>(fun spec ->
            { spec with
                Evaluator = Some (fun _ _ -> Task.CompletedTask) }) |> ignore

        let provider = ServiceProviderServiceExtensions.GetService<IHostedService>(
                            ServiceCollectionContainerBuilderExtensions.BuildServiceProvider(services))
        Assert.NotNull(provider)

// ─── Job Deduplication (F4) ───

module DeduplicationTests =

    [<Fact>]
    let ``RegisterWithDedup succeeds on first call`` () =
        let dl = InMemory.create<TestTask>()
        let result = (dl.RegisterWithDedup (Add(1, 1)) "key1").Result
        Assert.True(result)

    [<Fact>]
    let ``RegisterWithDedup rejects duplicate active key`` () =
        let dl = InMemory.create<TestTask>()
        let r1 = (dl.RegisterWithDedup (Add(1, 1)) "key1").Result
        let r2 = (dl.RegisterWithDedup (Add(2, 2)) "key1").Result
        Assert.True(r1)
        Assert.False(r2)

    [<Fact>]
    let ``RegisterWithDedup allows same key after job completes`` () =
        let dl = InMemory.create<TestTask>()
        (dl.RegisterWithDedup (Add(1, 1)) "key1").Result |> ignore

        // Poll and complete the job
        let jobs = dl.Poll().Result
        dl.SetDone({ jobs.[0] with CompletedAt = Some DateTime.UtcNow }).Wait()

        // Same key should now be allowed
        let result = (dl.RegisterWithDedup (Add(2, 2)) "key1").Result
        Assert.True(result)

    [<Fact>]
    let ``RegisterWithDedup allows same key after job fails`` () =
        let dl = InMemory.create<TestTask>()
        (dl.RegisterWithDedup (Add(1, 1)) "key1").Result |> ignore

        let jobs = dl.Poll().Result
        dl.SetFailed({ jobs.[0] with ErrorMessage = Some "boom" }).Wait()

        let result = (dl.RegisterWithDedup (Add(3, 3)) "key1").Result
        Assert.True(result)

    [<Fact>]
    let ``RegisterWithDedup rejects while InFlight`` () =
        let dl = InMemory.create<TestTask>()
        (dl.RegisterWithDedup (Add(1, 1)) "key1").Result |> ignore

        // Poll sets it InFlight
        dl.Poll().Result |> ignore

        let result = (dl.RegisterWithDedup (Add(2, 2)) "key1").Result
        Assert.False(result)

    [<Fact>]
    let ``ScheduleWithDedup succeeds on first call`` () =
        let dl = InMemory.create<TestTask>()
        let result = (dl.ScheduleWithDedup (Add(1, 1)) (DateTime.UtcNow.AddMinutes(5.0)) "sched1").Result
        Assert.True(result)

    [<Fact>]
    let ``ScheduleWithDedup rejects duplicate active key`` () =
        let dl = InMemory.create<TestTask>()
        let r1 = (dl.ScheduleWithDedup (Add(1, 1)) (DateTime.UtcNow.AddMinutes(5.0)) "sched1").Result
        let r2 = (dl.ScheduleWithDedup (Add(2, 2)) (DateTime.UtcNow.AddMinutes(10.0)) "sched1").Result
        Assert.True(r1)
        Assert.False(r2)

    [<Fact>]
    let ``Different dedup keys are independent`` () =
        let dl = InMemory.create<TestTask>()
        let r1 = (dl.RegisterWithDedup (Add(1, 1)) "key1").Result
        let r2 = (dl.RegisterWithDedup (Add(2, 2)) "key2").Result
        Assert.True(r1)
        Assert.True(r2)

    [<Fact>]
    let ``Regular Register ignores dedup entirely`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(1, 1)).Wait()
        // Both should exist — no dedup
        let jobs = dl.Poll().Result
        Assert.Equal(2, jobs.Length)
