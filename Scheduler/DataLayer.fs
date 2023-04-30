module DataLayer

open System

type IDataLayer<'t> =
    // Does any required setup like creating tables in database
    abstract member Create: unit -> unit

    // Registering must add the item 't into the pool of jobs
    // This job will be performed ASAP.
    // If the pool was a list it should be added to it: pool <- pool @ 't
    // If it is a database table it should be added to that table
    abstract member Register: 't -> unit

    // Schedule must add the item 't into the pool of jobs
    // This job will be performed at a given time
    // If the pool was a list it should be added to it: pool <- pool @ 't
    // If it is a database table it should be added to that table
    abstract member Schedule: 't -> DateTime option -> unit

    // Repeat must add the item 't into the pool of jobs
    // This job will be repeated at a certain interval
    // If the pool was a list it should be added to it: pool <- pool @ 't
    // If it is a database table it should be added to that table
    abstract member Repeat: 't -> unit -> unit

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

