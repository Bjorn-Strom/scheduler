<p align="center">
  <img src="logo.jpg" width="50%" />
</p>

### Steve

This library was inspired by scheduling systems like [Quartz](https://www.quartz-scheduler.net/) and [Hangfire](https://www.hangfire.io/). As I delved into the intricacies of these systems, I pondered the following questions:

- How can we ensure a scheduler is robust enough to avoid adding unwanted background tasks if our program crashes?
- Is it possible to provide a guarantee that a job will run, and if so, can we ensure it runs exactly once?
- Can I develop a system that remains backward compatible and won't disrupt existing jobs when tweaking functions and releasing new versions?

Driven by these considerations, I took on the challenge of implementing a simple database-driven scheduler in F#. The result of this endeavor is this library.

### Data Storage

**Steve** works with DataLayers, which is an abstraction around a data storage location for saving the background work data. Currently, the library provides the following data layers:

- **InMemory**: This data layer is designed for testing purposes and stores the data in memory.
- **MSSQL**: The MSSQL data layer allows you to store the data in a Microsoft SQL Server database.

It is also possible to create data layers that work with other databases or even different technologies such as MQTT. To create a custom data layer, simply implement the `IDataLayer` interface, and you'll be ready to integrate it with **Steve**.

### Reducer

In order to run functions without serializing them into the database, **Steve** utilizes a reducer.

To use the reducer, you need to define a discriminated union that represents your tasks. For example:

```fsharp
type SendEmail =
    | Invite of Participant * Event
    | Waitlist of Participant * Event
    | Cancel of Participant * Event
    ...
    ...
```

Next, you'll need to create an evaluator that handles each task. This evaluator is then passed to the scheduler. Here's an example:

```fsharp
let emailEvaluator =
    function
    | Invite (participant, event) ->
    // Call a function to send an invite email
    | Waitlist (participant, event) ->
    // Call a function to send a waitlist email
    | Cancel (participant, event) ->
    // Call a function to send a cancellation email
    ...
```

These evaluator functions can then perform the desired actions. This indirection between the stored tasks and the actual functions provides backward compatibility. As long as the system can deserialize the task, you can make changes to the underlying functions without breaking the deserialization process.

For example, if you need to support a new type of email, you can simply add it to the reducer. If you need to modify an existing task type, such as `Invite` needing only one parameter, you can introduce a temporary new task type like `Invite2` with the modified parameter list:

```fsharp
    | Invite of Participant * Event
    | Invite2 of Participant
```

As long as you stop registering jobs of the `Invite` type and switch to using `Invite2`, you can safely make the changes. Once the database no longer contains any `Invite` jobs, you can remove the event parameter. Finally, when there are no `Invite2` jobs left in the database, you can delete it and register jobs of type `Invite` only. This approach ensures minimal downtime and avoids losing any jobs.

The level of indirection provided by the reducer allows for flexibility and seamless transitions when modifying task definitions or underlying functions.


### Full example

```fsharp
let connection = new SqlConnection "connectionString"
connection.Open()

type MathReducer =
    | Add of int * int
    | Subtract of int * int
    | Multiply of int * int
    | Divide of int * int

let evaluate =
    function
    | Add (a, b) -> printfn $"{a + b}"
    | Subtract (a, b) -> printfn $"{a - b}"
    | Multiply (a, b) -> printfn $"{a * b}"
    | Divide (a, b) ->  printfn $"{a / b}"

let dataLayer = DataLayer.MSSQL.create<MathReducer>(connection)

schedulerBuilder<MathReducer> () {
    with_datalayer dataLayer
    with_polling_interval (TimeSpan.FromSeconds 1)
    with_max_jobs 1
    with_evaluator evaluate
}

// Schedule a job now
dataLayer.Register (Add (10, 10))
// Schedule a job in 5 minutes
dataLayer.Schedule (Multiply (10, 10)) (DateTime.Now.AddMinutes 5)
```

### Recurring Jobs

To schedule recurring jobs, you can add a new job in the reducer representing the recurrence pattern. Here's an example:

```fsharp
| Recurring ran ->
    let in1Hour = ran.AddHours 1
    datalayer.Schedule (Recurring in1Hour) in1Hour
    printfn $"It is now: {DateTime.Now}"
```

Note that for recurring jobs, you'll need to pass the data layer to the evaluator function.

### Options

**Steve** provides several options that you can use to customize the behavior of the scheduler:

- `with_datalayer`: Specifies the `IDataLayer` implementation to use.
- `with_polling_interval`: Specifies the interval, in seconds, for polling new jobs. The default value is 0 seconds.
- `with_max_jobs`: Specifies the maximum number of jobs that can run concurrently.
- `with_evaluator`: Specifies the evaluator to be used with this scheduler.

These options allow you to configure the data layer, polling interval, maximum job limit, and the evaluator function used by the scheduler according to your specific needs.
