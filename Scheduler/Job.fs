module Job

open System
open Dapper

type Status =
    | Done
    | Waiting
    | InFlight
    | Failed

    static member Serialize(status: Status) = status.ToString()

    static member Deserialize(s: string) =
        match s with
        | nameof Done -> Done
        | nameof Waiting -> Waiting
        | nameof InFlight -> InFlight
        | nameof Failed -> Failed

type StatusHandler() =
    inherit SqlMapper.TypeHandler<Status>()

    override __.Parse(value) = Status.Deserialize(string value)
    override __.SetValue(p, value) =
        p.DbType <- Data.DbType.String
        p.Size <- 16
        p.Value <- Status.Serialize value

[<CLIMutable>]
type Job =
    {
        Id: Guid
        Task: string
        Status: Status
        OnlyRunAfter: DateTime option
        LastUpdated: DateTime
    }

let create func onlyRunAfter =
    {
        Id = Guid.NewGuid()
        Task = Evaluator.serialize func
        Status = Waiting
        OnlyRunAfter = onlyRunAfter
        LastUpdated = DateTime.Now
    }

