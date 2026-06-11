<p align="center">
  <img src="logo.jpg" width="50%" />
</p>

### Steve

A simple database-driven job scheduler for F#.

Inspired by [Quartz](https://www.quartz-scheduler.net/) and [Hangfire](https://www.hangfire.io/), Steve focuses on:

- **Crash safety** — jobs live in the database, not in memory. If your process dies, pending work survives.
- **At-least-once execution** — jobs are atomically claimed when polled, so workers never race for the same job. The rare re-run (a crashed worker's job being reclaimed, or a slow worker outliving the stale timeout) means evaluators should be idempotent.
- **Backward compatibility** — tasks are serialized as discriminated unions. You can evolve your task types without breaking in-flight jobs.

### Data Layers

Steve works with `IDataLayer<'t>`, an abstraction over your job storage. Built-in implementations:

- **InMemory** — for testing.
- **MSSQL** — Microsoft SQL Server with automatic table creation, atomic poll-and-claim, and retry of transient errors (timeouts, deadlocks, throttling — 3 attempts with exponential backoff).

Custom data layers: implement `IDataLayer<'t>` to integrate any database or messaging system.

> **One table per task type.** The MSSQL data layer defaults to a table named `Scheduled_Jobs`. Two schedulers with different task types polling the same table will claim each other's jobs and fail to deserialize them. If you run multiple task types against one database, give each its own table:
>
> ```fsharp
> let instance = MSSQL.MSSQL<SendEmail>(connectionString, tableName = "Email_Jobs")
> (instance :> IDataLayer<SendEmail>).Setup().Wait()
> ```

### Evaluator Pattern

Define your tasks as a discriminated union:

```fsharp
type SendEmail =
    | Invite of Participant * Event
    | Waitlist of Participant * Event
    | Cancel of Participant * Event
```

Write an evaluator that handles each case. Evaluators receive a `CancellationToken` that is triggered on scheduler shutdown, and return `Task`:

```fsharp
let evaluate (t: SendEmail) (ct: CancellationToken) : Task = task {
    match t with
    | Invite (p, e) -> do! sendInviteEmail p e ct
    | Waitlist (p, e) -> do! sendWaitlistEmail p e ct
    | Cancel (p, e) -> do! sendCancelEmail p e ct
}
```

This indirection between stored tasks and actual functions provides backward compatibility. As long as the system can deserialize the task, you can change the underlying functions freely.

To evolve a task type (e.g., `Invite` no longer needs the `Event` parameter), add a temporary variant like `Invite2`, stop registering old `Invite` jobs, and remove the old variant once the database is drained.

### Full Example

```fsharp
open System
open System.Threading
open System.Threading.Tasks
open Steve

type MathTask =
    | Add of int * int
    | Subtract of int * int

let evaluate (t: MathTask) (_ct: CancellationToken) : Task = task {
    match t with
    | Add (a, b) -> printfn $"{a + b}"
    | Subtract (a, b) -> printfn $"{a - b}"
}

let dataLayer = MSSQL.create<MathTask>("connectionString")

let scheduler = schedulerBuilder<MathTask> () {
    with_datalayer dataLayer
    with_polling_interval (TimeSpan.FromSeconds 5)
    with_max_jobs 4
    with_evaluator evaluate
    with_max_retries 3
    with_logger myLogger
}

// Register a job (runs ASAP)
dataLayer.Register(Add(10, 10)).Wait()

// Schedule a job for later
dataLayer.Schedule (Subtract(20, 5)) (DateTime.UtcNow.AddMinutes 30.0) |> fun t -> t.Wait()

// Graceful shutdown — waits for in-flight jobs to finish
scheduler.Stop().Wait()
```

### Transaction-Safe Registration

Register jobs within an existing database transaction. Jobs only appear if the transaction commits:

```fsharp
use conn = new SqlConnection(connectionString)
conn.Open()
use txn = conn.BeginTransaction()

// Your business logic...
dataLayer.RegisterSafe (SendEmail.Invite(participant, event)) txn |> fun t -> t.Wait()

txn.Commit() // Job is now visible to the scheduler
```

### Job Deduplication

Prevent duplicate active jobs using a dedup key. If a `Waiting` or `InFlight` job with the same key exists, the call returns `false` and no new job is inserted:

```fsharp
let added = dataLayer.RegisterWithDedup (SendEmail.Invite(p, e)) "invite-123" |> fun t -> t.Result
// added = true (first time)

let duplicate = dataLayer.RegisterWithDedup (SendEmail.Invite(p, e)) "invite-123" |> fun t -> t.Result
// duplicate = false (already active)
```

Once the job completes or fails, the same key can be reused. `ScheduleWithDedup` works identically for scheduled jobs.

### Recurring Jobs

Register a recurring definition once (e.g. at startup); the scheduler enqueues an occurrence as a normal job whenever it's due:

```fsharp
do! dataLayer.UpsertRecurring
        { Name = "cleanup"
          Task = CleanupExpiredSessions
          Schedule = Every (TimeSpan.FromHours 1.0) }

do! dataLayer.UpsertRecurring
        { Name = "nightly-report"
          Task = SendReport
          Schedule = Cron "0 3 * * *" }  // standard 5-field cron, evaluated in UTC
```

The definition is the durable thing, so a failed (or even permanently failed) occurrence never stops the next one from firing. Semantics:

- **Upsert by name.** Re-upserting the same name and schedule keeps the next run time (safe to call on every startup); changing the schedule recomputes it.
- **No overlapping runs.** Occurrences carry the dedup key `recurring:<name>` — if the previous occurrence is still Waiting or InFlight, the new one is skipped and the schedule simply moves on.
- **No catch-up floods.** If the scheduler was down across several due times, the definition fires once and continues from now.
- **`RemoveRecurring "cleanup"`** deletes the definition; already-enqueued occurrences are unaffected.

`Every` intervals are measured from each enqueue, so they drift slightly; use `Cron` for wall-clock precision. Occurrences retry like any other job per `with_max_retries`.

### Options

| Option                  | Description                                                                      | Default        |
| ----------------------- | -------------------------------------------------------------------------------- | -------------- |
| `with_datalayer`        | `IDataLayer<'t>` implementation                                                  | Required       |
| `with_evaluator`        | `'t -> CancellationToken -> Task` function that processes jobs                   | Required       |
| `with_polling_interval` | How often to poll for new jobs (the first poll runs immediately)                 | 5 seconds      |
| `with_max_jobs`         | Max concurrent jobs — each poll only claims as many jobs as there are free slots | Unlimited      |
| `with_max_retries`      | Times to retry a failed job (exponential backoff)                                | 0 (no retries) |
| `with_stale_timeout`    | Reclaim InFlight jobs older than this duration                                   | Disabled       |
| `with_logger`           | `ILogger` for structured logging                                                 | No logging     |

### Graceful Shutdown

`schedulerBuilder` returns a `SchedulerHandle` with a `Stop()` method. Calling `Stop()` cancels the `CancellationToken` passed to evaluators and returns a `Task` that completes once all in-flight jobs finish. No new jobs are started after shutdown is requested.

Jobs that observe the cancellation (throw `OperationCanceledException`) are released back to `Waiting` and will be re-processed after the next start — they are not marked `Done`. Jobs that were claimed but never started are also released.

`SchedulerHandle` also implements `IAsyncDisposable`.

### Job Retries

Set `with_max_retries` to automatically retry failed jobs with exponential backoff (2^n seconds). After exhausting retries, the job is marked `Failed`.

```fsharp
schedulerBuilder<MyTask> () {
    with_datalayer dl
    with_evaluator evaluate
    with_max_retries 3  // retry up to 3 times (1s, 2s, 4s backoff)
}
```

### Stale Job Reclamation

If a worker crashes mid-job, InFlight jobs get stuck. Use `with_stale_timeout` to automatically reclaim them:

```fsharp
schedulerBuilder<MyTask> () {
    with_datalayer dl
    with_evaluator evaluate
    with_stale_timeout (TimeSpan.FromMinutes 10.0)
}
```

Jobs InFlight longer than the timeout are reset to `Waiting` and re-processed on the next poll.

Pick a timeout comfortably longer than your slowest job. If a worker is merely slow — not dead — and the timeout fires, the job runs twice: once on the slow worker, once wherever it gets reclaimed. This is the at-least-once trade-off; idempotent evaluators make it harmless.

### IHostedService Integration

Register Steve as a hosted service in ASP.NET Core / Generic Host:

```fsharp
open Steve

services.AddSteve<MyTask>(fun spec ->
    { spec with
        DataLayer = Some myDataLayer
        PollingInterval = TimeSpan.FromSeconds 5
        Evaluator = Some evaluate })
```

The scheduler starts with the host and stops gracefully on shutdown.

### Dashboard

`Steve.Dashboard` provides a web UI for monitoring and managing jobs. One-line setup in any ASP.NET Core app:

```fsharp
app.MapSteveDashboard(dataLayer :?> IDashboardDataLayer, "/steve") |> ignore
```

Features:

- **Stats overview** — waiting, in-flight, done, failed counts, average duration, throughput
- **Job table** — sortable columns, status filtering, pagination
- **Error visibility** — click to expand full error messages and task payloads
- **Actions** — retry failed jobs, requeue any job, delete individual jobs, bulk purge old completed jobs

The dashboard reads from `IDashboardDataLayer`, which both built-in data layers (InMemory and MSSQL) implement. No extra configuration needed.

> **The dashboard has no authentication.** Anyone who can reach the URL can retry, requeue, delete and purge jobs. Put it behind your own auth middleware (e.g. `RequireAuthorization` on the route group, or mount it only on an internal-facing host).

### Logging

Pass any `ILogger` implementation via `with_logger`. Steve logs:

- **Debug**: Job started, job completed
- **Information**: Poll returned N jobs, scheduler stopped
- **Warning**: Job scheduled for retry (with retry count), stale jobs reclaimed
- **Error**: Job failed (with exception), job failed after exhausting retries, poll failed, SetDone/SetFailed/SetRetry failures
- **Critical**: MailboxProcessor crashed with unhandled exception
