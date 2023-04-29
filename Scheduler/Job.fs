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

type Job =
    {
        Id: Guid
        SerializedTask: string
        Type: Type
        Status: Status
         // TODO: hva er et godt uttrykk for NÅR noe skal gjøres?
         // TODO: Dersom den er Single vil vi kun kjøre den engang, kanskje et tidspunkt?
         // TODO: Dersom den er recurring så annenhver dag? CRON?
         // TODO: Kanskje denne skal være optional? Some tid -> kjør da, None -> kjør nå
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

