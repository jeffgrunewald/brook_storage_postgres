defmodule Brook.Storage.Postgres do
  @moduledoc """
  Implements the `Brook.Storage` behaviour for the Postgres
  database, saving the application view state key/value pairs
  as JSONB records and the event structs that generate those
  key/value pairs as compressed binary encodings of in a BYTEA
  database type with optional truncation of events by type.
  """
  use GenServer
  require Logger
  import Brook.Config, only: [registry: 1, put: 3, get: 2]
  alias Brook.Storage.Postgres.Query

  # count = select count (*) from table, match out 'rows' field, flatten list
  # delete records sorted by create_ts = delete from events_table where id in (select id from events_table order by id limit number_rows_to_delete)
  # need to limit event types by event_limits map
  @behaviour Brook.Storage

  @impl Brook.Storage
  def persist(instance, event, collection, key, value) do
    %{postgrex: postgrex, schema: schema, table: table, event_limits: event_limits} =
      state(instance)

    Logger.debug(fn ->
      "#{__MODULE__}: persisting #{collection}:#{key}:#{inspect(value)} to postgres"
    end)

    with {:ok, serialized_event} <- Brook.Serializer.serialize(event),
         gzipped_serialized_event <- :zlib.gzip(serialized_event),
         event_limit <- Map.get(event_limits, event.type, :no_limit),
         {:ok, serialized_value} <- Brook.Serializer.serialize(value),
         :ok <-
           Query.postgres_upsert(postgrex, view_table(schema, table), collection, key, %{
             "key" => key,
             "value" => serialized_value
           }),
         :ok <-
           Query.postgres_insert_event(
             postgrex,
             events_table(schema, table),
             collection,
             key,
             event.type,
             event.create_ts,
             gzipped_serialized_event
           ) do
      :ok
    end
  rescue
    ArgumentError -> {:error, not_initialized_exception()}
  end

  @impl Brook.Storage
  def delete(instance, collection, key) do
    %{postgrex: postgrex, schema: schema, table: table} = state(instance)

    Query.postgres_delete(postgrex, view_table(schema, table), collection, key)
  end

  @impl Brook.Storage
  def get(instance, collection, key) do
    %{postgrex: postgrex, schema: schema, table: table} = state(instance)

    case Query.postgres_get(postgrex, view_table(schema, table), collection, key) do
      {:ok, []} ->
        {:ok, nil}

      {:ok, [serialized_value]} ->
        serialized_value
        |> Map.get("value")
        |> Brook.Deserializer.deserialize()

      error_result ->
        error_result
    end
  end

  @impl Brook.Storage
  def get_all(instance, collection) do
    %{postgrex: postgrex, schema: schema, table: table} = state(instance)

    with {:ok, serialized_values} <-
           Query.postgres_get(postgrex, view_table(schema, table), collection) do
      serialized_values
      |> Enum.map(&deserialize_data/1)
      |> Enum.into(%{})
      |> ok()
    end
  end

  @impl Brook.Storage
  def get_events(instance, collection, key) do
    %{postgrex: postgrex, schema: schema, table: table} = state(instance)

    with {:ok, compressed_events} <-
           Query.postgres_get_events(postgrex, events_table(schema, table), collection, key),
         serialized_events <- Enum.map(compressed_events, &:zlib.gunzip/1),
         {:ok, events} <- safe_map(serialized_events, &Brook.Deserializer.deserialize/1) do
      events |> sort_events() |> ok()
    end
  end

  @impl Brook.Storage
  def get_events(instance, collection, key, type) do
    %{postgrex: postgrex, schema: schema, table: table} = state(instance)

    with {:ok, compressed_events} <-
           Query.postgres_get_events(postgrex, events_table(schema, table), collection, key, type),
         serialized_events <- Enum.map(compressed_events, &:zlib.gunzip/1),
         {:ok, events} <- safe_map(serialized_events, &Brook.Deserializer.deserialize/1) do
      events |> sort_events() |> ok()
    end
  end

  @impl Brook.Storage
  def start_link(args) do
    instance = Keyword.fetch!(args, :instance)
    GenServer.start_link(__MODULE__, args, name: via(registry(instance)))
  end

  @impl GenServer
  def init(args) do
    instance = Keyword.fetch!(args, :instance)
    schema = Keyword.get(args, :schema, "public")
    table = Keyword.fetch!(args, :table)

    state = %{
      schema: schema,
      table: table,
      postgrex_args: Keyword.fetch!(args, :postgrex_args),
      event_limits: Keyword.get(args, :event_limits, %{})
    }

    {:ok, postgrex} = Postgrex.start_link(state.postgrex_args)

    put(instance, __MODULE__, %{
      schema: schema,
      table: table,
      postgrex: postgrex,
      event_limits: state.event_limits
    })

    {:ok, Map.put(state, :postgrex, postgrex), {:continue, :init_tables}}
  end

  @impl GenServer
  def handle_continue(:init_tables, %{schema: schema, table: table} = state) do
    view_table = view_table(schema, table)
    events_table = events_table(schema, table)

    with :ok <- Query.schema_create(state.postgrex, schema),
         :ok <- Query.view_table_create(state.postgrex, view_table),
         :ok <- Query.events_table_create(state.postgrex, view_table, events_table) do
      :ok
    else
      {:error, error} -> Logger.warn(fn -> "Unable to initialize tables : #{inspect(error)}" end)
    end

    {:noreply, state}
  end

  defp state(instance) do
    case get(instance, __MODULE__) do
      {:ok, value} -> value
      :error -> raise not_initialized_exception()
    end
  end

  defp safe_map(list, function) do
    Enum.reduce_while(list, {:ok, []}, fn value, {:ok, list} ->
      case function.(value) do
        {:ok, result} -> {:cont, {:ok, [result | list]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp ok(value), do: {:ok, value}

  defp view_table(schema, table), do: "#{schema}.#{table}"
  defp events_table(schema, table), do: "#{schema}.#{table}_events"

  defp deserialize_data(%{"key" => key, "value" => value}) do
    {:ok, deserialized_value} = Brook.Deserializer.deserialize(value)
    {key, deserialized_value}
  end

  defp sort_events(events) do
    Enum.sort_by(events, fn event -> event.create_ts end)
  end

  defp not_initialized_exception() do
    Brook.Uninitialized.exception(message: "#{__MODULE__} is not yet initialized!")
  end

  defp via(registry), do: {:via, Registry, {registry, __MODULE__}}
end
