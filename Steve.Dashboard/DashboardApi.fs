namespace Steve.Dashboard

open System
open System.Text.Json
open System.Text.Json.Serialization
open Steve.DataLayer

module DashboardApi =
    let internal jsonOptions =
        let opts = JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)
        opts.Converters.Add(JsonFSharpConverter())
        opts

    let getJobs (dl: IDashboardDataLayer) (status: string option) (page: int option) (pageSize: int option) =
        task {
            let parsedStatus =
                status |> Option.bind (fun s ->
                    match Steve.Job.Status.Deserialize s with
                    | Ok st -> Some st
                    | Error _ -> None)
            let query = { Status = parsedStatus; Page = defaultArg page 1; PageSize = defaultArg pageSize 20 }
            let! (jobs, total) = dl.QueryJobs(query)
            return {| jobs = jobs; total = total; page = query.Page; pageSize = query.PageSize |}
        }

    let getStats (dl: IDashboardDataLayer) = dl.GetStats()

    let retryJob (dl: IDashboardDataLayer) (id: Guid) = dl.RetryJob(id)

    let requeueJob (dl: IDashboardDataLayer) (id: Guid) = dl.RequeueJob(id)

    let deleteJob (dl: IDashboardDataLayer) (id: Guid) = dl.DeleteJob(id)

    let purgeJobs (dl: IDashboardDataLayer) (status: string) (olderThanMinutes: int) =
        task {
            match Steve.Job.Status.Deserialize status with
            | Error e -> return Error e
            | Ok s ->
                let! count = dl.PurgeJobs s (TimeSpan.FromMinutes(float olderThanMinutes))
                return Ok {| deleted = count |}
        }
