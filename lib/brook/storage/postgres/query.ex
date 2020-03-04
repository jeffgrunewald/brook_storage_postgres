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

  @spec postgres_upsert(
          conn(),
          schema_table(),
          Brook.view_collection(),
          Brook.view_key(),
          Brook.view_value()
        ) :: :ok | {:error, Brook.reason()}
  def postgres_upsert(conn, view_table, collection, key, value) do
    case Postgrex.query(
           conn,
           "INSERT INTO #{view_table} (collection, key, value)
        VALUES ($1, $2, $3)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value;",
           [collection, key, value]
         ) do
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
    case Postgrex.query(
           conn,
           "INSERT INTO #{events_table} (collection, key_id, type, create_ts, data)
        VALUES ($1, $2, $3, $4, $5);",
           [collection, key, type, timestamp, event]
         ) do
      {:ok, %Postgrex.Result{num_rows: 1}} -> :ok
      error_result -> error_result
    end
  end

  @spec postgres_delete(conn(), schema_table(), Brook.view_collection(), Brook.view_key()) ::
          :ok | {:error, Brook.reason()}
  def postgres_delete(conn, view_table, collection, key) do
    case Postgrex.query(
           conn,
           "DELETE FROM #{view_table}
        WHERE collection = $1
        AND key = $2;",
           [collection, key]
         ) do
      {:ok, %Postgrex.Result{num_rows: 1, rows: nil}} -> :ok
      error_result -> error_result
    end
  end

  @spec postgres_get(conn(), schema_table(), Brook.view_collection(), Brook.view_key() | nil) ::
          {:ok, [Brook.view_value()]} | {:error, Brook.reason()}
  def postgres_get(conn, view_table, collection, key \\ nil) do
    {key_variable, key_filter} =
      case key do
        nil -> {nil, []}
        _ -> {"AND key = $2", [key]}
      end

    case Postgrex.query(
           conn,
           "SELECT value
          FROM #{view_table}
          WHERE collection = $1
          #{key_variable}
          ;",
           [collection] ++ key_filter
         ) do
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
    {type_variable, type_filter} =
      case type do
        nil -> {nil, []}
        _ -> {"AND type = $3", [type]}
      end

    case Postgrex.query(
           conn,
           "SELECT data
        FROM #{events_table}
        WHERE collection = $1
        AND key_id = $2
        #{type_variable}
        ORDER BY create_ts ASC;",
           [collection, key] ++ type_filter
         ) do
      {:ok, %Postgrex.Result{rows: rows}} -> {:ok, List.flatten(rows)}
      error_result -> error_result
    end
  end

  @spec schema_create(conn(), String.t()) :: :ok | {:error, Brook.reason()}
  def schema_create(conn, schema) do
    case Postgrex.query(conn, "CREATE SCHEMA IF NOT EXISTS #{schema};", []) do
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
           Postgrex.query(
             conn,
             "CREATE TABLE IF NOT EXISTS #{view_table} (
             key VARCHAR PRIMARY KEY,
             collection VARCHAR,
             value JSONB
             );",
             []
           ) do
      Logger.info(fn -> "Table #{view_table} created with indices : key" end)

      :ok
    else
      {:ok, %Postgrex.Result{messages: [%{code: "42P07"}]}} ->
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
    with {:ok, %Postgrex.Result{messages: []}} <-
           Postgrex.query(
             conn,
             "CREATE TABLE IF NOT EXISTS #{events_table} (
             id BIGSERIAL PRIMARY KEY,
             key_id VARCHAR NOT NULL,
             collection VARCHAR,
             type VARCHAR,
             create_ts BIGINT,
             data BYTEA,
             FOREIGN KEY (key_id) REFERENCES #{view_table}(key) ON DELETE CASCADE
             );",
             []
           ),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(
             conn,
             "CREATE INDEX CONCURRENTLY type_idx ON #{events_table} (type);",
             []
           ),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(
             conn,
             "CREATE INDEX CONCURRENTLY timestamp_idx ON #{events_table} (create_ts);",
             []
           ) do
      Logger.info(fn ->
        "Table #{events_table} created with indices : type, create_ts"
      end)

      :ok
    else
      {:ok, %Postgrex.Result{messages: [%{code: "42P07"}]}} ->
        Logger.info(fn ->
          "Table #{events_table} already exists; skipping index creation"
        end)

        :ok

      error ->
        error
    end
  end
end
