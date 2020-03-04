defmodule BrookPostgres.Query do
  @moduledoc """
  Abstracts the Postgrex SQL query functions away from
  the storage behaviour implementation.

  Collects the command run directly against the Postgrex
  API in a single location for creates, inserts, selects,
  and deletes.
  """
  require Logger

  @spec postgres_upsert(pid(), String.t(), String.t(), String.t(), String.t()) :: :ok
  def postgres_upsert(conn, schema_table, collection, key, value) do
    {:ok, %Postgrex.Result{num_rows: 1}} =
      Postgrex.query(
        conn,
        "INSERT INTO #{schema_table} (collection, key, value)
        VALUES ($1, $2, $3)
        ON CONFLICT (key)
        DO UPDATE SET value = EXCLUDED.value;",
        [collection, key, value]
      )

    :ok
  end

  @spec postgres_insert_event(pid(), String.t(), String.t(), String.t(), String.Chars.t(), non_neg_integer(), binary()) :: :ok
  def postgres_insert_event(conn, schema_table, collection, key, type, timestamp, event) do
    {:ok, %Postgrex.Result{num_rows: 1}} =
      Postgrex.query(
        conn,
        "INSERT INTO #{schema_table}_events (collection, key, type, create_ts, data)
        VALUES ($1, $2, $3, $4, $5);",
        [collection, key, type, timestamp, event]
      )

    :ok
  end

  @spec postgres_delete(pid(), String.t(), String.t(), String.t()) :: :ok
  def postgres_delete(conn, schema_table, collection, key) do
    {:ok, %Postgrex.Result{num_rows: 1, rows: nil}} =
      Postgrex.query(
        conn,
        "DELETE FROM #{schema_table}
        WHERE collection = $1
        AND key = $2;",
        [collection, key]
      )

    :ok
  end

  @spec postgres_get(pid(), String.t(), String.t(), String.t() | nil) :: {:ok, term()}
  def postgres_get(conn, schema_table, collection, key \\ nil) do
    {key_variable, key_filter} =
      case key do
        nil -> {nil, []}
        _ -> {"AND key = $2", [key]}
      end

    {:ok, %Postgrex.Result{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT value
        FROM #{schema_table}
        WHERE collection = $1
        #{key_filter}
        ;",
        [collection] ++ key_filter
      )

    {:ok, List.flatten(rows)}
  end

  @spec postgres_get_events(pid(), String.t(), String.t(), String.t(), String.t() | nil) ::
          {:ok, [binary()]}
  def postgres_get_events(conn, schema_table, collection, key, type \\ nil) do
    {type_variable, type_filter} =
      case type do
        nil -> {nil, []}
        _ -> {"AND type = $3", [type]}
      end

    {:ok, %Postgrex.Result{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT data
        FROM #{schema_table}
        WHERE collection = $1
        AND key = $2
        #{type_variable}
        ORDER BY create_ts ASC;",
        [collection, key] ++ type_filter
      )

    {:ok, List.flatten(rows)}
  end

  @spec schema_create(pid(), String.t()) :: :ok | {:error, term}
  def schema_create(conn, schema) do
    case Postgrex.query(conn, "CREATE SCHEMA IF NOT EXISTS #{schema};", []) do
      {:ok, %Postgrex.Result{}} ->
        Logger.info(fn -> "Schema #{schema} successfully created" end)
        :ok

      error ->
        error
    end
  end

  @spec view_table_create(pid(), String.t(), String.t()) :: :ok | {:error, term()}
  def view_table_create(conn, schema, table) do
    with {:ok, %Postgrex.Result{messages: []}} <-
           Postgrex.query(
             conn,
             "CREATE TABLE IF NOT EXISTS #{schema}.#{table} (
             key VARCHAR PRIMARY KEY,
             collection VARCHAR,
             value JSONB
             );",
             []
           ) do
      Logger.info(fn -> "Table #{table} created in schema #{schema} with indices : key" end)
      :ok
    else
      {:ok, %Postgrex.Result{messages: [%{code: "42P07"}]}} ->
        Logger.info(fn ->
          "Table #{table} in schema #{schema} already exists; skipping index creation"
        end)

        :ok

      error ->
        error
    end
  end

  @spec events_table_create(pid(), String.t(), String.t()) :: :ok | {:error, term()}
  def events_table_create(conn, schema, table) do
    with {:ok, %Postgrex.Result{messages: []}} <-
           Postgrex.query(
             conn,
             "CREATE TABLE IF NOT EXISTS #{schema}.#{table}_events (
             id BIGSERIAL PRIMARY KEY,
             key_id VARCHAR NOT NULL,
             collection VARCHAR,
             type VARCHAR,
             create_ts BIGINT,
             data BYTEA,
             FOREIGN KEY (key_id) REFERENCES #{schema}.#{table}(key) ON DELETE CASCADE
             );",
             []
           ),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(
             conn,
             "CREATE INDEX CONCURRENTLY type_idx ON #{schema}.#{table}_events (type);",
             []
           ),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(
             conn,
             "CREATE INDEX CONCURRENTLY timestamp_idx ON #{schema}.#{table}_events (create_ts);",
             []
           ) do
      Logger.info(fn ->
        "Table #{table}_events created in schema #{schema} with indices : type, create_ts"
      end)

      :ok
    else
      {:ok, %Postgrex.Result{messages: [%{code: "42P07"}]}} ->
        Logger.info(fn ->
          "Table #{table}_events in schema #{schema} already exists; skipping index creation"
        end)

      error ->
        error
    end
  end
end
