namespace Steve

open System
open System.Globalization
open Cronos

/// When a recurring job fires.
type Schedule =
    /// A fixed interval between occurrences, measured from the time each occurrence is enqueued.
    | Every of TimeSpan
    /// A standard 5-field cron expression, evaluated in UTC.
    | Cron of string

module Schedule =

    let serialize (schedule: Schedule) =
        match schedule with
        | Every interval -> $"""every:{interval.ToString("c", CultureInfo.InvariantCulture)}"""
        | Cron expression -> $"cron:{expression}"

    let deserialize (s: string) =
        match s.Split(':', 2) with
        | [| "every"; interval |] -> Every(TimeSpan.ParseExact(interval, "c", CultureInfo.InvariantCulture))
        | [| "cron"; expression |] -> Cron expression
        | _ -> failwith $"Invalid schedule: '{s}'"

    /// The next time the schedule fires after the given UTC time.
    /// None means the schedule never fires again.
    /// Throws if a cron expression is malformed — call this at registration time to validate early.
    let nextOccurrence (schedule: Schedule) (afterUtc: DateTime) =
        match schedule with
        | Every interval -> Some(afterUtc + interval)
        | Cron expression ->
            let cron = CronExpression.Parse expression
            cron.GetNextOccurrence(DateTime.SpecifyKind(afterUtc, DateTimeKind.Utc))
            |> Option.ofNullable
