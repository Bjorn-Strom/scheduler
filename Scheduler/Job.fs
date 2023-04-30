module Job

open System

type Type =
    | Recurring
    | Single

type Status =
    | Done
    | Waiting
    | InFlight
    | Failed

// TODO: Trenger noe for å sette recurring her
type Job =
    {
        Id: Guid
        SerializedTask: string
        Type: Type
        Status: Status
        OnlyRunAfter: DateTime option
        LastUpdated: DateTime
    }

let create func type' onlyRunAfter =
    {
        Id = Guid.NewGuid()
        SerializedTask = Evaluator.serialize func
        Type = type'
        Status = Waiting
        OnlyRunAfter = onlyRunAfter
        LastUpdated = DateTime.Now
    }

