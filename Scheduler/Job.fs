module Job

open System

// TODO: Scheduler burde lese køa inn fra en database (hvordan den tabellen populeres blir database spesifikke biblioteker? SqLite, MSSQL...?)
// TODO: Databasen bør leses periodisk og køa burde oppdateres
// TODO: Hva om funksjonen thrower? Fault tolerant mailbox? Les opp på at-least once
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

