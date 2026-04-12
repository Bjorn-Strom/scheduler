module Evaluator
    open System.Text.Json
    open System.Text.Json.Serialization

    let private options =
        let opts = JsonSerializerOptions()
        opts.Converters.Add(JsonFSharpConverter())
        opts

    let serialize (obj: 't) =
        JsonSerializer.Serialize(obj, options)

    let deserialize<'t> (str: string) =
        JsonSerializer.Deserialize<'t>(str, options)
