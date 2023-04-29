module DataLayer

open System

type IDataLayer<'t> =
    abstract member Register: 't -> unit
    abstract member Get: DateTime -> Job.Job list
    abstract member Update: Job.Job -> unit
    abstract member SetInFlight: Job.Job -> unit

