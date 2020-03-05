# BrookStoragePostgres

Brook Storage Postgres implements the Brook Storage plugin behaviour to provide
the View State persistence to Elixir applications including the Brook library
for event-driven distibuted application orchestration.

For more on Brook, you can review the [source](https://github.com/bbalser/brook)
and read the docs on [Hex.pm](https://hexdocs.pm/brook).

Brook Storage Postgres supports the following operations of the Brook storage behaviour:
  * `persist/5` for persisting data to the application's view state
    - Events can optionally by limited by type to prevent over-consumption of database disk
      space and keep queries to the events table efficient. When event lists are truncated,
      the oldest entries in the table by type are removed first.
  * `delete/3` for removing data from an application's view state
  * `get/3` for retrieving data from the view state
  * `get_all/2` for retrieving all data from a given collection within the app's view state
  * `get_events/3` for retrieving all events from a collection that contributed to defining
     the data stored at the given key in the view state.
  * `get_events/4` the same as `get_events/3` but adds the additional filter of only retrieving
    events that defined the value of a given key by the specified event type.

## Installation

Brook Storage Postgres can be included in your application by adding the following
to your mix.exs file.

```elixir
def deps do
  [
    {:brook_storage_postgres, "~> 0.1.0"}
  ]
end
```

## Configuration

From your application runtime or config files, or anywhere in your code where Brook
configuration is generated and passed to the Brook Supervisor `start_link/1` function,
include the following block specifying the storage module with the necessary information
to use Brook Storage Postgres:

```elixir
brook_config = [
  instance: ...instance name...
  driver: %{...driver config here...},
  handlers: [...handler list here...],
  storage: %{
    module: Brook.Storage.Postgres,
    init_arg: [
      table: "table_name",
      postgrex_args: [
        hostname: "db_host",
        username: "app_svc_name",
        password: "app_svc_password",
        database: "db_engine"
      ],
      event_limits: %{
        "data:something" => 100,
        "data:something:else" => 50
      }
    ]
  }
]
```

The docs be found at [https://hexdocs.pm/brook_storage_postgres](https://hexdocs.pm/brook_storage_postgres).

