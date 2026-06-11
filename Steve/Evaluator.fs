namespace Steve

module Evaluator =
    open System.Text.Json
    open System.Text.Json.Serialization

    // This format is the storage contract: queued jobs are stored as JSON and must
    // deserialize across deploys. The union encoding is pinned explicitly so a
    // future FSharp.SystemTextJson default change can't break in-flight jobs.
    let private converterOptions =
        JsonFSharpOptions
            .Default()
            .WithUnionAdjacentTag()
            .WithUnionAllowUnorderedTag()
            .WithUnwrapOption()
            .WithUnionUnwrapSingleCaseUnions()

    let private options =
        let opts = JsonSerializerOptions()
        opts.Converters.Add(JsonFSharpConverter(converterOptions))
        opts

    let serialize (value: 't) =
        JsonSerializer.Serialize(value, options)

    let deserialize<'t> (str: string) =
        JsonSerializer.Deserialize<'t>(str, options)
