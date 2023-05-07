module Evaluator
    open Newtonsoft.Json

    let serialize (obj: 't) =
        JsonConvert.SerializeObject obj

    let deserialize<'t> str =
        JsonConvert.DeserializeObject<'t> str
