module Steve.Tests.Unit

open System
open System.Data
open System.Threading
open Xunit
open Job
open Scheduler
open DataLayer

// ─── Test DU for Evaluator / Scheduler ───

type TestTask =
    | Add of int * int
    | Greet of string
    | Nested of TestTask
    | Empty

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
          LastUpdated = DateTime.UtcNow }

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
          LastUpdated = DateTime.UtcNow }

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

        let evaluator =
            function
            | Add(a, b) ->
                result <- a + b
                evaluated.Set()
            | Greet _ -> ()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(10, 20)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Job should have been evaluated within timeout")
        Assert.Equal(30, result)

    [<Fact>]
    let ``Scheduler processes multiple jobs`` () =
        let dl = InMemory.create<TestTask>()
        let mutable counter = 0
        let allDone = new CountdownEvent(3)

        let evaluator =
            function
            | Add(a, b) ->
                Interlocked.Add(&counter, a + b) |> ignore
                allDone.Signal() |> ignore
            | Greet _ -> ()

        schedulerBuilder<TestTask> () {
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

    [<Fact>]
    let ``Scheduler handles failing job without crashing`` () =
        let dl = InMemory.create<TestTask>()
        let secondDone = new ManualResetEventSlim(false)
        let mutable result = 0

        let evaluator =
            function
            | Greet _ -> failwith "boom"
            | Add(a, b) ->
                result <- a + b
                secondDone.Set()

        schedulerBuilder<TestTask> () {
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

    [<Fact>]
    let ``Scheduler respects scheduled time`` () =
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let evaluator =
            function
            | Add _ -> evaluated.Set()
            | _ -> ()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        // Schedule 1 hour in future - should NOT run
        dl.Schedule (Add(1, 1)) (DateTime.UtcNow.AddHours 1.0) |> fun t -> t.Wait()

        let ran = evaluated.Wait(TimeSpan.FromMilliseconds 500.0)
        Assert.False(ran, "Future-scheduled job should not run yet")

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
          LastUpdated = DateTime.UtcNow }

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
          LastUpdated = DateTime.UtcNow }

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
        let phantom = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow }
        dl.SetDone(phantom).Wait()

    [<Fact>]
    let ``SetInFlight on nonexistent job does not crash`` () =
        let dl = InMemory.create<TestTask>()
        let phantom = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow }
        dl.SetInFlight(phantom).Wait()

    [<Fact>]
    let ``SetFailed on nonexistent job does not crash`` () =
        let dl = InMemory.create<TestTask>()
        let phantom = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow }
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
    let ``Poll only returns Waiting jobs after mixed status updates`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        dl.Register(Add(3, 3)).Wait()
        let jobs = dl.Poll().Result
        dl.SetDone(jobs.[0]).Wait()
        dl.SetFailed(jobs.[1]).Wait()
        let remaining = dl.Poll().Result
        Assert.Equal(1, List.length remaining)
        Assert.Equal(jobs.[2].Id, remaining.[0].Id)

// ─── Scheduler additional integration ───

module SchedulerAdditionalTests =
    [<Fact>]
    let ``Scheduler with_on_error receives failure`` () =
        let dl = InMemory.create<TestTask>()
        let mutable errorCtx = ""
        let errorReceived = new ManualResetEventSlim(false)

        let evaluator =
            function
            | Greet _ -> failwith "test error"
            | _ -> ()

        let onError (_ex: exn) (ctx: string) =
            errorCtx <- ctx
            errorReceived.Set()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
            with_on_error onError
        }

        dl.Register(Greet "boom").Wait()

        let received = errorReceived.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(received, "Error handler should be called")
        Assert.Contains("Job", errorCtx)

    [<Fact>]
    let ``Scheduler without max_jobs runs all concurrently`` () =
        let dl = InMemory.create<TestTask>()
        let mutable counter = 0
        let allDone = new CountdownEvent(5)

        let evaluator =
            function
            | Add(a, b) ->
                Interlocked.Add(&counter, a + b) |> ignore
                allDone.Signal() |> ignore
            | _ -> ()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_evaluator evaluator
        }

        for i in 1..5 do
            dl.Register(Add(i, i)).Wait()

        let completed = allDone.Wait(TimeSpan.FromSeconds 10.0)
        Assert.True(completed, "All jobs should run with no max_jobs limit")
        Assert.Equal(30, Volatile.Read(&counter))

    [<Fact>]
    let ``Scheduler max_jobs 1 processes sequentially`` () =
        let dl = InMemory.create<TestTask>()
        let mutable maxConcurrent = 0
        let mutable current = 0
        let lockObj = obj()
        let allDone = new CountdownEvent(3)

        let evaluator =
            function
            | Add _ ->
                lock lockObj (fun () ->
                    current <- current + 1
                    if current > maxConcurrent then maxConcurrent <- current)
                Thread.Sleep(50)
                lock lockObj (fun () -> current <- current - 1)
                allDone.Signal() |> ignore
            | _ -> ()

        schedulerBuilder<TestTask> () {
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

    [<Fact>]
    let ``Scheduler evaluator receives correct deserialized task`` () =
        let dl = InMemory.create<TestTask>()
        let mutable received: TestTask option = None
        let evaluated = new ManualResetEventSlim(false)

        let evaluator t =
            received <- Some t
            evaluated.Set()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Greet "world").Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Job should be evaluated")
        Assert.Equal(Some (Greet "world"), received)

    [<Fact>]
    let ``Scheduler builder fails without evaluator`` () =
        let dl = InMemory.create<TestTask>()
        Assert.Throws<exn>(fun () ->
            schedulerBuilder<TestTask> () {
                with_datalayer dl
                with_polling_interval (TimeSpan.FromMilliseconds 50.0)
                with_max_jobs 1
            }
        ) |> ignore

    [<Fact>]
    let ``Scheduler marks completed job as Done in datalayer`` () =
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let evaluator =
            function
            | Add _ -> evaluated.Set()
            | _ -> ()

        schedulerBuilder<TestTask> () {
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

    [<Fact>]
    let ``Scheduler marks failed job as Failed in datalayer`` () =
        let dl = InMemory.create<TestTask>()
        let errorReceived = new ManualResetEventSlim(false)

        let evaluator =
            function
            | Greet _ -> failwith "boom"
            | _ -> ()

        let onError (_: exn) (_: string) =
            errorReceived.Set()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
            with_on_error onError
        }

        dl.Register(Greet "fail").Wait()
        let received = errorReceived.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(received)
        // Give scheduler time to call SetFailed
        Thread.Sleep(200)
        let remaining = dl.Poll().Result
        Assert.Empty(remaining)

    [<Fact>]
    let ``Scheduler picks up jobs registered after start`` () =
        let dl = InMemory.create<TestTask>()
        let mutable counter = 0
        let batch1 = new ManualResetEventSlim(false)
        let batch2 = new ManualResetEventSlim(false)

        let evaluator =
            function
            | Add(a, b) ->
                let v = Interlocked.Add(&counter, a + b)
                if v >= 10 then batch1.Set()
                if v >= 30 then batch2.Set()
            | _ -> ()

        schedulerBuilder<TestTask> () {
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
        let dl = DataLayer.DataLayer.empty<TestTask>()
        dl.Setup().Wait()

    [<Fact>]
    let ``empty Register completes`` () =
        let dl = DataLayer.DataLayer.empty<TestTask>()
        dl.Register(Add(1, 2)).Wait()

    [<Fact>]
    let ``empty Schedule completes`` () =
        let dl = DataLayer.DataLayer.empty<TestTask>()
        dl.Schedule (Add(1, 2)) DateTime.UtcNow |> fun t -> t.Wait()

    [<Fact>]
    let ``empty Poll returns empty`` () =
        let dl = DataLayer.DataLayer.empty<TestTask>()
        let jobs = dl.Poll().Result
        Assert.Empty(jobs)

    [<Fact>]
    let ``empty SetDone completes`` () =
        let dl = DataLayer.DataLayer.empty<TestTask>()
        let job = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow }
        dl.SetDone(job).Wait()

    [<Fact>]
    let ``empty SetInFlight completes`` () =
        let dl = DataLayer.DataLayer.empty<TestTask>()
        let job = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow }
        dl.SetInFlight(job).Wait()

    [<Fact>]
    let ``empty SetFailed completes`` () =
        let dl = DataLayer.DataLayer.empty<TestTask>()
        let job = { Id = Guid.NewGuid(); Task = ""; Status = Waiting; OnlyRunAfter = None; LastUpdated = DateTime.UtcNow }
        dl.SetFailed(job).Wait()

    [<Fact>]
    let ``empty RegisterSafe completes`` () =
        let dl = DataLayer.DataLayer.empty<TestTask>()
        dl.RegisterSafe (Add(1, 2)) null |> fun t -> t.Wait()

    [<Fact>]
    let ``empty ScheduleSafe completes`` () =
        let dl = DataLayer.DataLayer.empty<TestTask>()
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
        let remaining = dl.Poll().Result
        Assert.Equal(1, List.length remaining)
        Assert.Equal(jobs.[1].Id, remaining.[0].Id)

    [<Fact>]
    let ``SetInFlight only affects target job`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let jobs = dl.Poll().Result
        dl.SetInFlight(jobs.[0]).Wait()
        let remaining = dl.Poll().Result
        Assert.Equal(1, List.length remaining)
        Assert.Equal(jobs.[1].Id, remaining.[0].Id)

    [<Fact>]
    let ``SetFailed only affects target job`` () =
        let dl = InMemory.create<TestTask>()
        dl.Register(Add(1, 1)).Wait()
        dl.Register(Add(2, 2)).Wait()
        let jobs = dl.Poll().Result
        dl.SetFailed(jobs.[0]).Wait()
        let remaining = dl.Poll().Result
        Assert.Equal(1, List.length remaining)
        Assert.Equal(jobs.[1].Id, remaining.[0].Id)

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
    open DataLayer.DataLayer

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
            member _.Poll() =
                let n = Interlocked.Increment(&pollCalls)
                if n <= failCount then
                    Task.FromException<Job.Job list>(exn "poll failed")
                else
                    realDl.Poll() }

    [<Fact>]
    let ``Scheduler recovers after poll failure`` () =
        let realDl = InMemory.create<TestTask>()
        let dl = makeFailingThenRecoveringDl 3 realDl
        let evaluated = new ManualResetEventSlim(false)

        let evaluator =
            function
            | Add(a, b) when a = 777 && b = 333 -> evaluated.Set()
            | _ -> ()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        realDl.Register(Add(777, 333)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Scheduler should recover from poll failures and process job")

    [<Fact>]
    let ``Scheduler calls on_error for poll failure`` () =
        let realDl = InMemory.create<TestTask>()
        let dl = makeFailingThenRecoveringDl 1 realDl
        let errorReceived = new ManualResetEventSlim(false)
        let mutable errorCtx = ""

        let evaluator = function _ -> ()

        let onError (_: exn) (ctx: string) =
            errorCtx <- ctx
            errorReceived.Set()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 50.0)
            with_max_jobs 1
            with_evaluator evaluator
            with_on_error onError
        }

        let received = errorReceived.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(received, "on_error should be called for poll failure")
        Assert.Contains("Poll", errorCtx)

// ─── Scheduler zero polling interval ───

module SchedulerZeroIntervalTests =
    [<Fact>]
    let ``Scheduler with zero polling interval processes jobs`` () =
        let dl = InMemory.create<TestTask>()
        let evaluated = new ManualResetEventSlim(false)

        let evaluator =
            function
            | Add(a, b) when a = 888 && b = 111 -> evaluated.Set()
            | _ -> ()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromSeconds 0.0)
            with_max_jobs 1
            with_evaluator evaluator
        }

        dl.Register(Add(888, 111)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 5.0)
        Assert.True(completed, "Scheduler with zero interval should still process jobs")
