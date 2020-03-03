defmodule BrookPostgres.Storage do
  @moduledoc """
  Implements the `Brook.Storage` behaviour for the Postgres
  database, saving the application view state as serialized
  (binary) encodings of the direct Elixir terms to be saved
  with compression.
  """
  use GenServer
  require Logger
  import Brook.Config, only: [registry: 1, put: 3, get: 2]
  alias BrookPostgres.Query

  # count = select count (*) from table, match out 'rows' field, flatten list
  # delete records sorted by create_ts = delete from events_table where id in (select id from events_table order by id limit number_rows_to_delete)

  @behaviour Brook.Storage

  @impl Brook.Storage
  def persist(_instance, _event, _collection, _key, _value) do
    :ok

    # must compress event prior to storing
    # must store the event AND the value
    # must support different collections for event types AND limits on each type
    # may create a table of known types for events (author = string, create_ts = timestamp, data = jsonb, forwarded = boolean, type = string)
    # inserts values as jsonb (Brook serialize and then json decode to map)
  end

  @impl Brook.Storage
  def delete(_instance, _collection, _key) do
    :ok

    # must delete the stored key AND the events of that type for that collection
  end

  @impl Brook.Storage
  def get(_instance, _collection, key) do
    key

    # must get a given value from a given collection by its key
  end

  @impl Brook.Storage
  def get_all(_instance, _collection) do
    {:ok, %{}}

    # must get all values from a given collection
  end

  @impl Brook.Storage
  def get_events(instance, collection, key) do
    %{postgrex: postgrex, schema: schema, table: table, event_limits: event_limits} =
      state(instance)

    with {:ok, compressed_events} <-
           Query.postgres_get_events(postgrex, "#{schema}.#{table}", collection, key),
         serialized_events <- Enum.map(compressed_events, &:zlib.gunzip/1),
         {:ok, events} <- safe_map(serialized_events, &Brook.Deserializer.deserialize/1) do
      {:ok, events}
    end
  end

  @impl Brook.Storage
  def get_events(instance, collection, key, type) do
    %{postgrex: postgrex, schema: schema, table: table, event_limits: event_limits} =
      state(instance)

    with {:ok, compressed_events} <-
           Query.postgres_get_events(postgrex, "#{schema}.#{table}", collection, key, type),
         serialized_events <- Enum.map(compressed_events, &:zlib.gunzip/1),
         {:ok, events} <- safe_map(serialized_events, &Brook.Deserializer.deserialize/1) do
      {:ok, events}
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

    {:ok, %{state | postgrex: postgrex}, {:continue, :init_tables}}
  end

  @impl GenServer
  def handle_continue(:init_tables, state) do
    with :ok <- Query.schema_create(state.postgrex, state.schema),
         :ok <- Query.view_table_create(state.postgrex, state.schema, state.table),
         :ok <- Query.events_table_create(state.postgrex, state.schema, state.table) do
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

  defp not_initialized_exception() do
    Brook.Uninitialized.exception(message: "#{__MODULE__} is not yet initialized!")
  end

  defp via(registry), do: {:via, Registry, {registry, __MODULE__}}
end
