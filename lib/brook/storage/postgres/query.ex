defmodule Brook.Storage.Postgres.Query do
  @moduledoc """
  Abstracts the Postgrex SQL query functions away from
  the storage behaviour implementation.

  Collects the command run directly against the Postgrex
  API in a single location for creates, inserts, selects,
  and deletes.
  """

  @typedoc "The pid of the Postgrex connection process"
  @type conn :: pid

  @typedoc "The combination of the view application view schema and table in 'schema_name.table_name' format"
  @type schema_table :: String.t()

  @typedoc "The timestamp of an event's creation"
  @type timestamp :: non_neg_integer

  @typedoc "The serialized and compressed version of a Brook event"
  @type event :: binary()

  require Logger
  import Brook.Storage.Postgres.Statement

  @pg_already_exists "42P07"

  @spec postgres_upsert(
          conn(),
          schema_table(),
          Brook.view_collection(),
          Brook.view_key(),
          Brook.view_value()
        ) :: :ok | {:error, Brook.reason()}
  def postgres_upsert(conn, view_table, collection, key, value) do
    case Postgrex.query(conn, upsert_stmt(view_table), [collection, key, value]) do
      {:ok, %Postgrex.Result{num_rows: 1}} -> :ok
      error_result -> error_result
    end
  end

  @spec postgres_insert_event(
          conn(),
          schema_table(),
          Brook.view_collection(),
          Brook.view_key(),
          Brook.event_type(),
          timestamp(),
          event()
        ) :: :ok | {:error, Brook.reason()}
  def postgres_insert_event(conn, events_table, collection, key, type, timestamp, event) do
    case Postgrex.query(conn, insert_event_stmt(events_table), [
           collection,
           key,
           type,
           timestamp,
           event
         ]) do
      {:ok, %Postgrex.Result{num_rows: 1}} -> :ok
      error_result -> error_result
    end
  end

  @spec postgres_delete(conn(), schema_table(), Brook.view_collection(), Brook.view_key()) ::
          :ok | {:error, Brook.reason()}
  def postgres_delete(conn, view_table, collection, key) do
    case Postgrex.query(conn, delete_stmt(view_table), [collection, key]) do
      {:ok, %Postgrex.Result{num_rows: 1, rows: nil}} -> :ok
      error_result -> error_result
    end
  end

  @spec postgres_get(conn(), schema_table(), Brook.view_collection(), Brook.view_key() | nil) ::
          {:ok, [Brook.view_value()]} | {:error, Brook.reason()}
  def postgres_get(conn, view_table, collection, key \\ nil) do
    {key_filter, key_variable} = if key, do: {true, [key]}, else: {false, []}

    case Postgrex.query(conn, get_stmt(view_table, key_filter), [collection] ++ key_variable) do
      {:ok, %Postgrex.Result{rows: rows}} -> {:ok, List.flatten(rows)}
      error_result -> error_result
    end
  end

  @spec postgres_get_events(
          conn(),
          schema_table(),
          Brook.view_collection(),
          Brook.view_key(),
          Brook.event_type() | nil
        ) ::
          {:ok, [event()]} | {:error, Brook.reason()}
  def postgres_get_events(conn, events_table, collection, key, type \\ nil) do
    {type_filter, type_variable} = if type, do: {true, [type]}, else: {false, []}

    case Postgrex.query(
           conn,
           get_events_stmt(events_table, type_filter),
           [collection, key] ++ type_variable
         ) do
      {:ok, %Postgrex.Result{rows: rows}} -> {:ok, List.flatten(rows)}
      error_result -> error_result
    end
  end

  @spec schema_create(conn(), String.t()) :: :ok | {:error, Brook.reason()}
  def schema_create(conn, schema) do
    case Postgrex.query(conn, create_schema_stmt(schema), []) do
      {:ok, %Postgrex.Result{}} ->
        Logger.info(fn -> "Schema #{schema} successfully created" end)
        :ok

      error ->
        error
    end
  end

  @spec view_table_create(conn(), schema_table()) :: :ok | {:error, Brook.reason()}
  def view_table_create(conn, view_table) do
    with {:ok, %Postgrex.Result{messages: []}} <-
           Postgrex.query(conn, create_view_stmt(view_table), []) do
      Logger.info(fn -> "Table #{view_table} created with indices : key" end)

      :ok
    else
      {:ok, %Postgrex.Result{messages: [%{code: @pg_already_exists}]}} ->
        Logger.info(fn ->
          "Table #{view_table} already exists; skipping index creation"
        end)

        :ok

      error ->
        error
    end
  end

  @spec events_table_create(pid(), schema_table(), schema_table()) ::
          :ok | {:error, Brook.reason()}
  def events_table_create(conn, view_table, events_table) do
    type_field = "type"
    timestamp_field = "create_ts"

    with {:ok, %Postgrex.Result{messages: []}} <-
           Postgrex.query(conn, create_events_stmt(view_table, events_table), []),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(conn, create_index_stmt(events_table, type_field), []),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(conn, create_index_stmt(events_table, timestamp_field), []) do
      Logger.info(fn ->
        "Table #{events_table} created with indices : #{type_field}, #{timestamp_field}"
      end)

      :ok
    else
      {:ok, %Postgrex.Result{messages: [%{code: @pg_already_exists}]}} ->
        Logger.info(fn ->
          "Table #{events_table} already exists; skipping index creation"
        end)

        :ok

      error ->
        error
    end
  end
end
