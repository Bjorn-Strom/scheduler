module Evaluator
    open Newtonsoft.Json

    let serialize obj =
        JsonConvert.SerializeObject obj

    // TODO RESULT?
    let deserialize<'a> str =
        JsonConvert.DeserializeObject<'a> str
