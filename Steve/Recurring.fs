namespace Steve

module Recurring =

    open System

    /// A recurring job definition. The definition is the durable thing;
    /// each due occurrence is enqueued as a normal job.
    type RecurringJob<'t> =
        { /// Identifies the definition. Upserting with the same name replaces it.
          Name: string
          Task: 't
          Schedule: Schedule }

    [<CLIMutable>]
    type RecurringRecord =
        { Name: string
          Task: string
          Schedule: string
          NextRun: DateTime
          LastEnqueued: DateTime option }

    /// Occurrences carry this dedup key so a still-active previous
    /// occurrence suppresses the next one instead of stacking up.
    let dedupKey (name: string) = $"recurring:{name}"

    /// NextRun when a schedule has no future occurrence: effectively never.
    let private never = DateTime.MaxValue

    let createRecord (job: RecurringJob<'t>) (now: DateTime) =
        { Name = job.Name
          Task = Evaluator.serialize job.Task
          Schedule = Schedule.serialize job.Schedule
          NextRun =
            Schedule.nextOccurrence job.Schedule now
            |> Option.defaultValue never
          LastEnqueued = None }

    /// The job row to enqueue for a due definition.
    let occurrenceJob (definition: RecurringRecord) (now: DateTime) : Job.JobRecord =
        { Id = Guid.NewGuid()
          Task = definition.Task
          Status = Job.Waiting
          OnlyRunAfter = None
          LastUpdated = now
          RetryCount = 0
          StartedAt = None
          CompletedAt = None
          ErrorMessage = None
          DedupKey = Some(dedupKey definition.Name) }

    /// When the definition should fire next, computed from now rather than
    /// from the missed time — a scheduler that was down for five intervals
    /// fires once on catch-up, not five times.
    let advance (definition: RecurringRecord) (now: DateTime) =
        let next =
            Schedule.nextOccurrence (Schedule.deserialize definition.Schedule) now
            |> Option.defaultValue never
        { definition with NextRun = next; LastEnqueued = Some now }
