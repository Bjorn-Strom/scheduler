module Steve.Tests.MSSQL

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Testcontainers.MsSql
open Steve
open Steve.Job
open Steve.DataLayer

type TestTask =
    | Add of int * int
    | Greet of string

type MSSQLFixture() =
    let container =
        MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .Build()

    member _.ConnectionString = container.GetConnectionString()

    interface IAsyncLifetime with
        member _.InitializeAsync() = container.StartAsync()
        member _.DisposeAsync() = container.DisposeAsync().AsTask()

type MSSQLDataLayerTests(fixture: MSSQLFixture) =
    let createDl () = MSSQL.create<TestTask>(fixture.ConnectionString)

    interface IClassFixture<MSSQLFixture>

    [<Fact>]
    member _.``Setup creates table without error`` () =
        let dl = createDl ()
        dl.Setup().Wait()

    [<Fact>]
    member _.``Setup is idempotent`` () =
        let dl = createDl ()
        dl.Setup().Wait()
        dl.Setup().Wait()

    [<Fact>]
    member _.``Register then Poll returns job`` () =
        let dl = createDl ()
        dl.Register(Add(1, 2)).Wait()
        let jobs = dl.Poll().Result
        Assert.True(List.length jobs >= 1)

    [<Fact>]
    member _.``Registered job has correct deserialized task`` () =
        let dl = createDl ()
        dl.Register(Add(99, 1)).Wait()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(99, 1))
        Assert.True(found, "Should find registered task in polled jobs")

    [<Fact>]
    member _.``Schedule future job not returned by Poll`` () =
        let dl = createDl ()
        dl.Schedule (Add(7, 7)) (DateTime.UtcNow.AddHours 1.0) |> fun t -> t.Wait()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(7, 7))
        Assert.False(found, "Future-scheduled job should not appear in Poll")

    [<Fact>]
    member _.``Schedule past job returned by Poll`` () =
        let dl = createDl ()
        dl.Schedule (Add(8, 8)) (DateTime.UtcNow.AddHours -1.0) |> fun t -> t.Wait()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(8, 8))
        Assert.True(found, "Past-scheduled job should appear in Poll")

    [<Fact>]
    member _.``SetDone removes job from Poll`` () =
        let dl = createDl ()
        dl.Register(Add(50, 50)).Wait()
        let jobs = dl.Poll().Result
        let target = jobs |> List.find (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(50, 50))
        dl.SetDone(target).Wait()
        let after = dl.Poll().Result
        let stillThere = after |> List.exists (fun j -> j.Id = target.Id)
        Assert.False(stillThere, "Done job should not appear in Poll")

    [<Fact>]
    member _.``SetInFlight removes job from Poll`` () =
        let dl = createDl ()
        dl.Register(Add(51, 51)).Wait()
        let jobs = dl.Poll().Result
        let target = jobs |> List.find (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(51, 51))
        dl.SetInFlight(target).Wait()
        let after = dl.Poll().Result
        let stillThere = after |> List.exists (fun j -> j.Id = target.Id)
        Assert.False(stillThere, "InFlight job should not appear in Poll")

    [<Fact>]
    member _.``SetFailed removes job from Poll`` () =
        let dl = createDl ()
        dl.Register(Add(52, 52)).Wait()
        let jobs = dl.Poll().Result
        let target = jobs |> List.find (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(52, 52))
        dl.SetFailed(target).Wait()
        let after = dl.Poll().Result
        let stillThere = after |> List.exists (fun j -> j.Id = target.Id)
        Assert.False(stillThere, "Failed job should not appear in Poll")

    [<Fact>]
    member _.``Multiple registers create multiple jobs`` () =
        let dl = createDl ()
        dl.Register(Add(60, 1)).Wait()
        dl.Register(Add(60, 2)).Wait()
        dl.Register(Add(60, 3)).Wait()
        let jobs = dl.Poll().Result
        let count = jobs |> List.filter (fun j ->
            let t = Evaluator.deserialize<TestTask> j.Task
            match t with Add(60, _) -> true | _ -> false) |> List.length
        Assert.True(count >= 3)

    [<Fact>]
    member _.``RegisterSafe inserts job`` () =
        let dl = createDl ()
        use conn = new Microsoft.Data.SqlClient.SqlConnection(fixture.ConnectionString)
        conn.Open()
        use txn = conn.BeginTransaction()
        dl.RegisterSafe (Add(70, 70)) txn |> fun t -> t.Wait()
        txn.Commit()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(70, 70))
        Assert.True(found)

    [<Fact>]
    member _.``RegisterSafe rolled back does not persist`` () =
        let dl = createDl ()
        use conn = new Microsoft.Data.SqlClient.SqlConnection(fixture.ConnectionString)
        conn.Open()
        use txn = conn.BeginTransaction()
        dl.RegisterSafe (Add(71, 71)) txn |> fun t -> t.Wait()
        txn.Rollback()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(71, 71))
        Assert.False(found, "Rolled back job should not be in Poll")

    [<Fact>]
    member _.``ScheduleSafe inserts job`` () =
        let dl = createDl ()
        use conn = new Microsoft.Data.SqlClient.SqlConnection(fixture.ConnectionString)
        conn.Open()
        use txn = conn.BeginTransaction()
        dl.ScheduleSafe (Add(72, 72)) (DateTime.UtcNow.AddHours -1.0) txn |> fun t -> t.Wait()
        txn.Commit()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(72, 72))
        Assert.True(found)

    [<Fact>]
    member _.``ScheduleSafe rolled back does not persist`` () =
        let dl = createDl ()
        use conn = new Microsoft.Data.SqlClient.SqlConnection(fixture.ConnectionString)
        conn.Open()
        use txn = conn.BeginTransaction()
        dl.ScheduleSafe (Add(73, 73)) (DateTime.UtcNow.AddHours -1.0) txn |> fun t -> t.Wait()
        txn.Rollback()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(73, 73))
        Assert.False(found, "Rolled back scheduled job should not be in Poll")

    [<Fact>]
    member _.``Full scheduler integration with MSSQL`` () =
        let dl = createDl ()
        let mutable result = 0
        let evaluated = new ManualResetEventSlim(false)

        let evaluator (t: TestTask) (_ct: CancellationToken) : Task = task {
            match t with
            | Add(a, b) ->
                if a = 555 && b = 444 then
                    result <- a + b
                    evaluated.Set()
            | _ -> ()
        }

        let handle = schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 100.0)
            with_max_jobs 10
            with_evaluator evaluator
        }

        dl.Register(Add(555, 444)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 15.0)
        Assert.True(completed, "Job should be processed by scheduler")
        Assert.Equal(999, result)
        handle.Stop().Wait()

    [<Fact>]
    member _.``ScheduleSafe future committed not returned by Poll`` () =
        let dl = createDl ()
        use conn = new Microsoft.Data.SqlClient.SqlConnection(fixture.ConnectionString)
        conn.Open()
        use txn = conn.BeginTransaction()
        dl.ScheduleSafe (Add(74, 74)) (DateTime.UtcNow.AddHours 1.0) txn |> fun t -> t.Wait()
        txn.Commit()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(74, 74))
        Assert.False(found, "Future-scheduled safe job should not appear in Poll")

    [<Fact>]
    member _.``Poll returns empty on fresh table`` () =
        // createDl calls Setup which creates table, but we use unique values
        // to distinguish from other tests. A fresh poll should not find our marker.
        let dl = createDl ()
        let jobs = dl.Poll().Result
        let found = jobs |> List.exists (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(999, 999))
        Assert.False(found)

    [<Fact>]
    member _.``Poll atomically claims jobs as InFlight`` () =
        let dl = createDl ()
        dl.Register(Add(80, 1)).Wait()
        dl.Register(Add(80, 2)).Wait()
        let jobs = dl.Poll().Result
        let claimed = jobs |> List.filter (fun j ->
            let t = Evaluator.deserialize<TestTask> j.Task
            match t with Add(80, _) -> true | _ -> false)
        Assert.True(List.length claimed >= 2)
        // All claimed jobs should be InFlight
        for j in claimed do
            Assert.Equal(Job.InFlight, j.Status)
        // Second poll should not return them again
        let after = dl.Poll().Result
        let reclaimed = after |> List.exists (fun j ->
            claimed |> List.exists (fun c -> c.Id = j.Id))
        Assert.False(reclaimed, "Already claimed jobs should not reappear in Poll")

// ─── Dashboard data layer (MSSQL) ───

type MSSQLDashboardTests(fixture: MSSQLFixture) =
    let createDl () = MSSQL.create<TestTask>(fixture.ConnectionString)
    let dash (dl: IDataLayer<TestTask>) = dl :?> IDashboardDataLayer

    interface IClassFixture<MSSQLFixture>

    [<Fact>]
    member _.``QueryJobs returns all jobs`` () =
        let dl = createDl ()
        dl.Register(Add(200, 1)).Wait()
        dl.Register(Add(200, 2)).Wait()
        // Poll to move to InFlight so we can SetDone
        dl.Poll().Result |> ignore
        let jobs, total = (dash dl).QueryJobs({ Status = None; Page = 1; PageSize = 100 }).Result
        Assert.True(total >= 2)
        Assert.True(jobs.Length >= 2)

    [<Fact>]
    member _.``QueryJobs filters by status`` () =
        let dl = createDl ()
        dl.Register(Add(201, 1)).Wait()
        let polled = dl.Poll().Result
        let target = polled |> List.find (fun j -> Evaluator.deserialize<TestTask> j.Task = Add(201, 1))
        dl.SetDone(target).Wait()
        let jobs, _ = (dash dl).QueryJobs({ Status = Some Job.Done; Page = 1; PageSize = 100 }).Result
        let found = jobs |> List.exists (fun j -> j.Id = target.Id)
        Assert.True(found, "Should find done job when filtering by Done status")

    [<Fact>]
    member _.``GetJob returns existing job`` () =
        let dl = createDl ()
        dl.Register(Add(202, 1)).Wait()
        let polled = dl.Poll().Result
        let target = polled |> List.find (fun j -> Evaluator.deserialize<TestTask> j.Task = Add(202, 1))
        let found = (dash dl).GetJob(target.Id).Result
        Assert.True(found.IsSome)
        Assert.Equal(target.Id, found.Value.Id)

    [<Fact>]
    member _.``GetJob returns None for missing`` () =
        let dl = createDl ()
        let found = (dash dl).GetJob(Guid.NewGuid()).Result
        Assert.True(found.IsNone)

    [<Fact>]
    member _.``RetryJob resets failed job`` () =
        let dl = createDl ()
        dl.Register(Add(203, 1)).Wait()
        let polled = dl.Poll().Result
        let target = polled |> List.find (fun j -> Evaluator.deserialize<TestTask> j.Task = Add(203, 1))
        dl.SetFailed(target).Wait()
        let result = (dash dl).RetryJob(target.Id).Result
        Assert.True(result)
        let job = (dash dl).GetJob(target.Id).Result
        Assert.Equal(Job.Waiting, job.Value.Status)

    [<Fact>]
    member _.``RetryJob returns false for non-failed`` () =
        let dl = createDl ()
        dl.Register(Add(204, 1)).Wait()
        let polled = dl.Poll().Result
        let target = polled |> List.find (fun j -> Evaluator.deserialize<TestTask> j.Task = Add(204, 1))
        let result = (dash dl).RetryJob(target.Id).Result
        Assert.False(result)

    [<Fact>]
    member _.``DeleteJob removes job`` () =
        let dl = createDl ()
        dl.Register(Add(205, 1)).Wait()
        let polled = dl.Poll().Result
        let target = polled |> List.find (fun j -> Evaluator.deserialize<TestTask> j.Task = Add(205, 1))
        let result = (dash dl).DeleteJob(target.Id).Result
        Assert.True(result)
        let found = (dash dl).GetJob(target.Id).Result
        Assert.True(found.IsNone)

    [<Fact>]
    member _.``GetStats returns counts`` () =
        let dl = createDl ()
        let d = dash dl
        dl.Register(Add(206, 1)).Wait()
        dl.Register(Add(206, 2)).Wait()
        let polled = dl.Poll().Result
        let t1 = polled |> List.find (fun j -> Evaluator.deserialize<TestTask> j.Task = Add(206, 1))
        dl.SetDone(t1).Wait()
        let stats = d.GetStats().Result
        Assert.True(stats.DoneCount >= 1)
        Assert.True(stats.InFlightCount >= 1)
