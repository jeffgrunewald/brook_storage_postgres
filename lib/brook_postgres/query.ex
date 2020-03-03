defmodule BrookPostgres.Query do
  @moduledoc """
  Abstracts the Postgrex SQL query functions away from
  the storage behaviour implementation.

  Collects the command run directly against the Postgrex
  API in a single location for creates, inserts, selects,
  and deletes.
  """
  require Logger

  @spec postgres_get(pid(), String.t(), String.t(), String.t() | nil) :: {:ok, term()}
  def postgres_get(conn, schema_table, collection, key \\ nil) do
    key_filter =
      case key do
        nil -> nil
        _ -> "AND key = '#{key}'"
      end

    {:ok, %Postgrex.Result{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT value
        FROM #{schema_table}
        WHERE collection = '#{collection}'
        #{key_filter}
        ;",
        []
      )

    {:ok, List.flatten(rows)}
  end

  @spec postgres_get_events(pid(), String.t(), String.t(), String.t(), String.t() | nil) ::
          {:ok, [binary()]}
  def postgres_get_events(conn, schema_table, collection, key, type \\ nil) do
    type_filter =
      case type do
        nil -> nil
        _ -> "AND type = '#{type}'"
      end

    {:ok, %Postgrex.Result{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT data
        FROM #{schema_table}
        WHERE collection = '#{collection}'
        AND key = '#{key}'
        #{type_filter}
        ORDER BY create_ts ASC;",
        []
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
             id SERIAL PRIMARY KEY,
             collection VARCHAR,
             key VARCHAR,
             value JSONB
             );",
             []
           ),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(
             conn,
             "CREATE INDEX CONCURRENTLY key_idx ON #{schema}.#{table} (key);",
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
             key_id INTEGER NOT NULL,
             collection VARCHAR,
             type VARCHAR,
             author VARCHAR,
             create_ts TIMESTAMP,
             forwarded BOOLEAN,
             data BYTEA,
             FOREIGN KEY (key_id) REFERENCES #{schema}.#{table}(id) ON DELETE CASCADE
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
