namespace Steve.Dashboard

open System
open System.IO
open System.Reflection
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System.Text.Json
open System.Threading.Tasks
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Routing
open Steve.DataLayer

[<Extension>]
type SteveDashboardExtensions =

    /// <summary>
    /// Maps the Steve dashboard routes and UI to the given prefix.
    /// The data layer must implement IDashboardDataLayer (built-in MSSQL and InMemory do).
    /// </summary>
    [<Extension>]
    static member MapSteveDashboard(app: IEndpointRouteBuilder, dl: IDashboardDataLayer, [<Optional; DefaultParameterValue("/steve")>] prefix: string) =
        let prefix = prefix.TrimEnd('/')

        // Serve embedded HTML dashboard
        app.MapGet(prefix, Func<HttpContext, Task>(fun ctx -> task {
            let assembly = Assembly.GetExecutingAssembly()
            use stream = assembly.GetManifestResourceStream("Steve.Dashboard.dashboard.html")
            if isNull stream then
                ctx.Response.StatusCode <- 404
            else
                ctx.Response.ContentType <- "text/html; charset=utf-8"
                do! stream.CopyToAsync(ctx.Response.Body)
        })) |> ignore

        // GET /api/jobs?status=&page=&pageSize=
        app.MapGet(prefix + "/api/jobs", Func<HttpContext, Task>(fun ctx -> task {
            let status = ctx.Request.Query.["status"] |> Seq.tryHead
            let page = ctx.Request.Query.["page"] |> Seq.tryHead |> Option.bind (fun s -> match Int32.TryParse s with true, v -> Some v | _ -> None)
            let pageSize = ctx.Request.Query.["pageSize"] |> Seq.tryHead |> Option.bind (fun s -> match Int32.TryParse s with true, v -> Some v | _ -> None)
            let! result = DashboardApi.getJobs dl status page pageSize
            ctx.Response.ContentType <- "application/json"
            do! JsonSerializer.SerializeAsync(ctx.Response.Body, result, DashboardApi.jsonOptions)
        })) |> ignore

        // GET /api/stats
        app.MapGet(prefix + "/api/stats", Func<HttpContext, Task>(fun ctx -> task {
            let! stats = DashboardApi.getStats dl
            ctx.Response.ContentType <- "application/json"
            do! JsonSerializer.SerializeAsync(ctx.Response.Body, stats, DashboardApi.jsonOptions)
        })) |> ignore

        // POST /api/jobs/{id}/retry
        app.MapPost(prefix + "/api/jobs/{id}/retry", Func<HttpContext, Task>(fun ctx -> task {
            let idStr = ctx.Request.RouteValues.["id"] :?> string
            match Guid.TryParse idStr with
            | true, id ->
                let! success = DashboardApi.retryJob dl id
                ctx.Response.ContentType <- "application/json"
                if success then
                    do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| ok = true |}, DashboardApi.jsonOptions)
                else
                    ctx.Response.StatusCode <- 404
                    do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| ok = false; error = "Job not found or not in Failed state" |}, DashboardApi.jsonOptions)
            | _ ->
                ctx.Response.StatusCode <- 400
                do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| error = "Invalid job ID" |}, DashboardApi.jsonOptions)
        })) |> ignore

        // POST /api/jobs/{id}/requeue
        app.MapPost(prefix + "/api/jobs/{id}/requeue", Func<HttpContext, Task>(fun ctx -> task {
            let idStr = ctx.Request.RouteValues.["id"] :?> string
            match Guid.TryParse idStr with
            | true, id ->
                let! success = DashboardApi.requeueJob dl id
                ctx.Response.ContentType <- "application/json"
                if success then
                    do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| ok = true |}, DashboardApi.jsonOptions)
                else
                    ctx.Response.StatusCode <- 404
                    do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| ok = false; error = "Job not found" |}, DashboardApi.jsonOptions)
            | _ ->
                ctx.Response.StatusCode <- 400
                do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| error = "Invalid job ID" |}, DashboardApi.jsonOptions)
        })) |> ignore

        // DELETE /api/jobs/{id}
        app.MapDelete(prefix + "/api/jobs/{id}", Func<HttpContext, Task>(fun ctx -> task {
            let idStr = ctx.Request.RouteValues.["id"] :?> string
            match Guid.TryParse idStr with
            | true, id ->
                let! success = DashboardApi.deleteJob dl id
                ctx.Response.ContentType <- "application/json"
                if success then
                    do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| ok = true |}, DashboardApi.jsonOptions)
                else
                    ctx.Response.StatusCode <- 404
                    do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| ok = false; error = "Job not found" |}, DashboardApi.jsonOptions)
            | _ ->
                ctx.Response.StatusCode <- 400
                do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| error = "Invalid job ID" |}, DashboardApi.jsonOptions)
        })) |> ignore

        // DELETE /api/jobs?status=Done&olderThanMinutes=10080
        app.MapDelete(prefix + "/api/jobs", Func<HttpContext, Task>(fun ctx -> task {
            let status = ctx.Request.Query.["status"] |> Seq.tryHead
            let olderThan = ctx.Request.Query.["olderThanMinutes"] |> Seq.tryHead |> Option.bind (fun s -> match Int32.TryParse s with true, v -> Some v | _ -> None)
            match status, olderThan with
            | Some s, Some m ->
                let! result = DashboardApi.purgeJobs dl s m
                ctx.Response.ContentType <- "application/json"
                match result with
                | Ok r -> do! JsonSerializer.SerializeAsync(ctx.Response.Body, r, DashboardApi.jsonOptions)
                | Error e ->
                    ctx.Response.StatusCode <- 400
                    do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| error = e |}, DashboardApi.jsonOptions)
            | _ ->
                ctx.Response.StatusCode <- 400
                do! JsonSerializer.SerializeAsync(ctx.Response.Body, {| error = "status and olderThanMinutes query parameters required" |}, DashboardApi.jsonOptions)
        })) |> ignore

        app
