defmodule BrookPostgres.Storage do
  @moduledoc """
  Implements the `Brook.Storage` behaviour for the Postgres
  database, saving the application view state as serialized
  (binary) encodings of the direct Elixir terms to be saved
  with compression.
  """
  use GenServer
  require Logger

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
  def get_events(_instance, _collection, _key) do
    {:ok, []}

    # must get all events from a given collection and key
  end

  @impl Brook.Storage
  def get_events(_instance, _collection, _key, _type) do
    {:ok, []}

    # must get all events from a given collection and key of the specified type
  end

  @impl Brook.Storage
  def start_link(_args) do
    :ok

    # returns a genserver with a name registered to the brook registry
  end

  @impl GenServer
  def init(_args) do
    :ok

    # stores the state in the genserver including the brook instance, postgres args, brook namespace, and event_limits for event types
  end
end
