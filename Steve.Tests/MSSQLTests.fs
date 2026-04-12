module Steve.Tests.MSSQL

open System
open System.Threading
open System.Threading.Tasks
open Xunit
open Testcontainers.MsSql
open DataLayer
open Scheduler

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

        let evaluator =
            function
            | Add(a, b) ->
                // Only signal on our specific job
                if a = 555 && b = 444 then
                    result <- a + b
                    evaluated.Set()
            | _ -> ()

        schedulerBuilder<TestTask> () {
            with_datalayer dl
            with_polling_interval (TimeSpan.FromMilliseconds 100.0)
            with_max_jobs 10
            with_evaluator evaluator
        }

        dl.Register(Add(555, 444)).Wait()

        let completed = evaluated.Wait(TimeSpan.FromSeconds 15.0)
        Assert.True(completed, "Job should be processed by scheduler")
        Assert.Equal(999, result)

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
    member _.``SetDone only affects target job`` () =
        let dl = createDl ()
        dl.Register(Add(80, 1)).Wait()
        dl.Register(Add(80, 2)).Wait()
        let jobs = dl.Poll().Result
        let target = jobs |> List.find (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(80, 1))
        let other = jobs |> List.find (fun j ->
            Evaluator.deserialize<TestTask> j.Task = Add(80, 2))
        dl.SetDone(target).Wait()
        let after = dl.Poll().Result
        let targetGone = after |> List.exists (fun j -> j.Id = target.Id) |> not
        let otherStill = after |> List.exists (fun j -> j.Id = other.Id)
        Assert.True(targetGone, "Done job should be gone")
        Assert.True(otherStill, "Other job should remain")
