# datafusion-distributed-experiment

This is an experiment for adding distributed execution capabilities to DataFusion. It introduces 
several core components that allow query plans to be executed across multiple physical machines. Rather than
providing an out-of-the-box solution, it aims to be the foundational building blocks over which
distributed engines can be built using DataFusion.


## Architecture

There are two main core components that allow a plan to be distributed:
- `ArrowFlightReadExec`: an `ExecutionPlan` implementation that, instead of directly executing its children
  as any other `ExecutionPlan`, it serializes them and sends them over the wire to an Arrow Flight endpoint
  for the rest of the plan to be executed there.
- `ArrowFlightEndpoint`: a `FlightService` implementation based on https://github.com/hyperium/tonic that
  listens for serialized DataFusion `ExecutionPlan`s, executes them in a DataFusion runtime, and sends the
  resulting Arrow `RecordBatch` stream to the caller.

There's a many-to-many relationship between these two core components:
- A single `ArrowFlightReadExec` can gather data from multiple `ArrowFlightEndpoint`
- A single `ArrowFlightEndpoint` can stream data back to multiple `ArrowFlightReadExec`

## Example

Imagine we have a plan that looks like this:
```
   ┌──────────────────────┐
   │    ProjectionExec    │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    DataSourceExec    │
   └──────────────────────┘  
```

And we want to distribute the aggregation, to something like this:
```
                              ┌──────────────────────┐
                              │    ProjectionExec    │
                              └──────────────────────┘  
                                         ▲
                              ┌──────────┴───────────┐
                              │    AggregateExec     │
                              │       (final)        │
                              └──────────────────────┘  
                                       ▲ ▲ ▲
              ┌────────────────────────┘ │ └─────────────────────────┐
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │    AggregateExec     │   │    AggregateExec     │   │    AggregateExec     │
   │      (partial)       │   │      (partial)       │   │      (partial)       │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘
              ▲                          ▲                           ▲
              └────────────────────────┐ │ ┌─────────────────────────┘
                              ┌────────┴─┴─┴─────────┐
                              │    DataSourceExec    │
                              └──────────────────────┘
```

Where each partial `AggregateExec` executes on a physical machine.

This can be done be injecting the appropriate `ArrowFlightReadExec` nodes in the appropriate places:
```
                              ┌──────────────────────┐
                              │    ProjectionExec    │
                              └──────────────────────┘  
                                         ▲
                              ┌──────────┴───────────┐
                              │    AggregateExec     │
                              │       (final)        │
                              └──────────────────────┘  
                                         ▲
                              ┌──────────┴───────────┐
                              │  ArrowFlightReadExec │
                              └──────────────────────┘  
                                       ▲ ▲ ▲                                         ┐
              ┌────────────────────────┘ │ └─────────────────────────┐               │
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐    │
   │ ArrowFlightEndpoint  │   │ ArrowFlightEndpoint  │   │ ArrowFlightEndpoint  │    │
   └──────────┬───────────┘   └──────────┬───────────┘   └───────────┬──────────┘    │
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐    │
   │    AggregateExec     │   │    AggregateExec     │   │    AggregateExec     │    │
   │      (partial)       │   │      (partial)       │   │      (partial)       │    │ stage 1
   └──────────┬───────────┘   └──────────┬───────────┘   └───────────┬──────────┘    │
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐    │
   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │    │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘    │
              ▲                          ▲                           ▲               │
              └────────────────────────┐ │ ┌─────────────────────────┘               ┘
                              ┌────────┴─┴─┴─────────┐                               ┐
                              │  ArrowFlightEndpoint │                               │
                              └──────────┬───────────┘                               │ stage 2
                              ┌──────────┴───────────┐                               │
                              │    DataSourceExec    │                               │ 
                              └──────────────────────┘                               ┘
```

Now, let's zoom in into the first distribution step for the plan above:

```
                              ┌──────────┴───────────┐
                              │  ArrowFlightReadExec │
                              └──────────────────────┘  
                                       ▲ ▲ ▲
              ┌────────────────────────┘ │ └─────────────────────────┐
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │ ArrowFlightEndpoint  │   │ ArrowFlightEndpoint  │   │ ArrowFlightEndpoint  │
   └──────────┬───────────┘   └──────────┬───────────┘   └───────────┬──────────┘
```


There's only one `ArrowFlightReadExec`, so in this case it's straightforward, no coordination 
is necessary between multiple `ArrowFlightReadExec`, as there's only one, and can just issue
3 queries to 3 different `ArrowFlightEndpoint`. The 3 specific `ArrowFlightEndpoint` urls do
not even need to be resolved at planning time, `ArrowFlightReadExec` can just rely on whatever
load balancing mechanism existed on the first place to go to three different `ArrowFlightEndpoint`s.


Now, let's look at the last distribution step:

```
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘
              ▲                          ▲                           ▲
              └────────────────────────┐ │ ┌─────────────────────────┘
                              ┌────────┴─┴─┴─────────┐
                              │  ArrowFlightEndpoint │
                              └──────────┬───────────┘
```

Each `ArrowFlightReadExec` need to go to exactly the same `ArrowFlightEndpoint`, so in this case
some coordination is needed. The 3 different `ArrowFlightReadExec` cannot just query a random
`ArrowFlightEndpoint`, it needs to be the same, and the `ArrowFlightEndpoint` will need to know
how to fanout its data into 3 different partitions.

This is the only place where some actual coordination is needed, and to understand how it's
done, some terminology needs to be explained first:
- **Stage**: A stage is a chunk of nodes that are getting executed in parallel potentially in different machines.
  In the third diagram above, the different stages for the distributed queries are shown on the right.
- **Actor**: An actor, is each one of the `ArrowFlightReadExec` nodes in a stage that need to coordinate together
  to go to the next stage.
- **Delegate**: A delegate is an actor that was chosen by the previous stage to act as a "leader", and communicate
  to all its peer actors information relevant about the next stage

Going back to the diagram above, imagine the `ArrowFlightEndpoint` below is the one listening on IP `10.0.0.25`:
```
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │
   │      (delegate)      │   │       (actor)        │   │        (actor)       │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘
                                                                      
                                                                      
                              ┌──────────────────────┐
                              │  ArrowFlightEndpoint │
                              │      10.0.0.25       │
                              └──────────┬───────────┘
```

First, the delegate resolves its IP:

```
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │
   │      (delegate)      │   │       (actor)        │   │        (actor)       │
   └──────────┬───────────┘   └──────────────────────┘   └──────────────────────┘
          next stage
      actors: [10.0.0.25]
              └────────────────────────┐
                              ┌──────────────────────┐
                              │  ArrowFlightEndpoint │
                              │      10.0.0.25       │
                              └──────────┬───────────┘
```

Then, the delegate communicates its actor colleagues how does the next stage look like:
```
                             next stage                 next stage
                        actors: [10.0.0.25]        actors: [10.0.0.25]
                        ┌────────┬──────────────────────────┐
                        │        ▼                          ▼        
   ┌──────────┴─────────┴─┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │
   │      (delegate)      │   │       (actor)        │   │        (actor)       │
   └──────────┬───────────┘   └──────────────────────┘   └──────────────────────┘
          next stage             
      actors: [10.0.0.25]   
              └────────────────────────┐
                              ┌──────────────────────┐
                              │  ArrowFlightEndpoint │
                              │      10.0.0.25       │
                              └──────────┬───────────┘
```

Then, all actors start querying the same `ArrowFlightEndpoint`

```
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │
   │      (delegate)      │   │       (actor)        │   │        (actor)       │
   └──────────┬───────────┘   └──────────┬───────────┘   └───────────┬──────────┘
          next stage                 next stage                 next stage
      actors: [10.0.0.25]        actors: [10.0.0.25]         actors: [10.0.0.25]
              └────────────────────────┐ │ ┌─────────────────────────┘
                              ┌──────────────────────┐
                              │  ArrowFlightEndpoint │
                              │      10.0.0.25       │
                              └──────────┬───────────┘
```

But wait, how do the `delegate` know what are the IPs of its peer actors? the `delegate` cannot
just go to two other random nodes hoping that they actually belong to the same stage.

This is solved by propagating two things a `StageContext` between stages that contain information about
what actors are involved in the stage, and this must have been done at the beginning, when the 
first stage was created here.

```
                              ┌──────────────────────┐
                              │    ProjectionExec    │
                              └──────────────────────┘  
                                         ▲
                              ┌──────────┴───────────┐
                              │    AggregateExec     │
                              │       (final)        │
                              └──────────────────────┘  
                                         ▲
                              ┌──────────┴───────────┐
                              │  ArrowFlightReadExec │ <- This one is responsible for propagating the StageContext
                              └──────────────────────┘  
                                       ▲ ▲ ▲                                         ┐
              ┌────────────────────────┘ │ └─────────────────────────┐               │
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐    │
   │ ArrowFlightEndpoint  │   │ ArrowFlightEndpoint  │   │ ArrowFlightEndpoint  │    │  stage 1
   └──────────┬───────────┘   └──────────┬───────────┘   └───────────┬──────────┘    │
```


In practice, two pieces of information are propagated:
- `StageContext`: information about the actors involved in stage and who is the delegate of that stage. This information
   is the same for all the actors inside the same stage.
- `ActorContext`: information about the actual actor running part of the query. This allows different actors inside a
   stage to self-identify and know whether they are the delegate or they are plain actors.

Zooming in into the `ArrowFlightReadExec` node, it will look like this:

```
                              ┌──────────┴───────────┐
                              │  ArrowFlightReadExec │
                              └──────────────────────┘  
                                     next stage             
                    actors: [10.0.0.10, 10.0.0.11, 10.0.0.12]
              ┌────────────────────────┘ │ └─────────────────────────┐           
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │ ArrowFlightEndpoint  │   │ ArrowFlightEndpoint  │   │ ArrowFlightEndpoint  │
   │      10.0.0.10       │   │      10.0.0.11       │   │       10.0.0.12      │
   └──────────┬───────────┘   └──────────┬───────────┘   └───────────┬──────────┘
        StageContext:              StageContext:               StageContext:
         actors: [                   actors: [                   actors: [  
          10.0.0.10,                   10.0.0.10,                  10.0.0.10,
          10.0.0.11,                   10.0.0.11,                  10.0.0.11,
          10.0.0.12                    10.0.0.12                   10.0.0.12 
         ]                           ]                           ]          
         delegate: 0                 delegate: 0                 delegate: 0
        ActorContext:              ActorContext:               ActorContext:
         actor_idx: 0                actor_idx: 1                actor_idx: 2
```

With this, the delegate of the first stage has all the information necessary for communicating details
about the next stage (the second one) to its peer actors.
```
              │
       StageContext:
        actors: [  
         10.0.0.10,
         10.0.0.11,
         10.0.0.12
        ] 
        delegate: 0     ┌────────┬──────────────────────────┐
       ActorContext:    │    10.0.0.11                  10.0.0.12
        actor_idx: 0    │        ▼                          ▼        
   ┌──────────┴─────────┴─┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │   │ ArrowFlightReadExec  │
   │      (delegate)      │   │       (actor)        │   │        (actor)       │
   └──────────┬───────────┘   └──────────────────────┘   └──────────────────────┘
              └────────────────────────┐
                              ┌──────────────────────┐
                              │  ArrowFlightEndpoint │
                              │      10.0.0.25       │
                              └──────────┬───────────┘
```


# Example

There's an example about how this looks like in [tests/localhost.rs](./tests/localhost.rs)