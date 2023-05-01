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
        | nameof Done -> Ok Done
        | nameof Waiting -> Ok Waiting
        | nameof InFlight -> Ok InFlight
        | nameof Failed -> Ok Failed
        | _ -> Error $"Invalid status: '{s}'"

type StatusHandler() =
    inherit SqlMapper.TypeHandler<Status>()

    override _.Parse(value) =
        match Status.Deserialize(string value) with
        | Ok status -> status
        | Error e -> failwith e

    override _.SetValue(p, value) =
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

