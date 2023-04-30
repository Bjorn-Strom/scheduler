module DataLayer

open System
open System.Data

type IDataLayer<'t> =
    // Does any required setup like creating tables in database
    abstract member Setup: unit -> unit

    // Registering must add the item 't into the pool of jobs
    // This job will be performed ASAP.
    // If the pool was a list it should be added to it: pool <- pool @ 't
    // If it is a database table it should be added to that table
    abstract member Register: 't -> unit
    abstract member RegisterSafe: 't -> IDbTransaction -> unit

    // Schedule must add the item 't into the pool of jobs
    // This job will be performed at a given time
    // If the pool was a list it should be added to it: pool <- pool @ 't
    // If it is a database table it should be added to that table
    abstract member Schedule: 't -> DateTime option -> unit
    abstract member ScheduleSafe: 't -> DateTime option -> IDbTransaction -> unit

    // Repeat must add the item 't into the pool of jobs
    // This job will be repeated at a certain interval
    // If the pool was a list it should be added to it: pool <- pool @ 't
    // If it is a database table it should be added to that table
    // TODO: MÅ FIKSE INTERVAL
    abstract member Repeat: 't -> unit -> unit
    abstract member RepeatSafe: 't -> unit -> IDbTransaction -> unit

    // Get should get all jobs in the pool.
    // If a datetime is supplied then get all jobs where the OnlyRunAfter date is after the date supplied
    abstract member Get: DateTime option -> Job.Job list

    // Updates a specific job and marks it as done
    // Should update last updated
    abstract member SetDone: Job.Job -> unit

    // Updates a specific job and marks it as in flight
    // Should update last updated
    abstract member SetInFlight: Job.Job -> unit

    // Updates a specific job and marks it as failed
    // Should update last updated
    abstract member SetFailed: Job.Job -> unit

let empty(): IDataLayer<'t> = {
    new IDataLayer<'t> with
        member this.Setup() = ()
        member this.Register var0 = ()
        member this.Schedule var0 var1 = ()
        member this.Repeat var0 var1 = ()
        member this.Get var0 = []
        member this.SetDone var0 = ()
        member this.SetInFlight var0 = ()
        member this.SetFailed var0 = ()
        member this.RegisterSafe var0 var1 = ()
        member this.RepeatSafe var0 var1 var2 = ()
        member this.ScheduleSafe var0 var1 var2 = ()
}

