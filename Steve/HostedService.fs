namespace Steve

open System.Threading
open System.Threading.Tasks
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.DependencyInjection

type SteveHostedService<'t>(configure: Scheduler.SchedulerSpec<'t> -> Scheduler.SchedulerSpec<'t>) =
    let mutable handle: Scheduler.SchedulerHandle option = None

    interface IHostedService with
        member _.StartAsync(_ct: CancellationToken) =
            let spec = Scheduler.empty<'t>() |> configure
            let builder = Scheduler.SchedulerBuilder<'t>()
            handle <- Some (builder.Run(spec))
            Task.CompletedTask

        member _.StopAsync(_ct: CancellationToken) =
            match handle with
            | Some h -> h.Stop()
            | None -> Task.CompletedTask

[<AutoOpen>]
module HostedServiceExtensions =
    type IServiceCollection with
        member services.AddSteve<'t>(configure: Scheduler.SchedulerSpec<'t> -> Scheduler.SchedulerSpec<'t>) =
            services.AddSingleton<IHostedService>(SteveHostedService<'t>(configure))
