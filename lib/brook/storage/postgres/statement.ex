defmodule Brook.Storage.Postgres.Statement do
  @moduledoc """
  Encapsulates the SQL statements BrookStoragePostgres
  submits to the database as functions for easier maintenance.
  """

  def upsert_stmt(table) do
    ~s|
    INSERT INTO #{table} (collection, key, value)
    VALUES ($1, $2, $3)
    ON CONFLICT (key)
    DO UPDATE SET value = EXCLUDED.value;
    |
  end

  def insert_event_stmt(table) do
    ~s|
    INSERT INTO #{table} (collection, key_id, type, create_ts, data)
    VALUES ($1, $2, $3, $4, $5);
    |
  end

  def delete_stmt(table) do
    ~s|
    DELETE FROM #{table}
    WHERE COLLECTION = $1
    AND KEY = $2;
    |
  end

  def get_stmt(table, key \\ false) do
    key_variable = if key, do: ~s|AND key = $2|, else: nil

    ~s|
    SELECT value
    FROM #{table}
    WHERE collection = $1
    #{key_variable};
    |
  end

  def get_events_stmt(table, type \\ false) do
    type_variable = if type, do: ~s|AND type = $3|, else: nil

    ~s|
    SELECT data
    FROM #{table}
    WHERE collection = $1
    AND key_id = $2
    #{type_variable}
    ORDER BY create_ts ASC;
    |
  end

  def create_schema_stmt(schema) do
    ~s|CREATE SCHEMA IF NOT EXISTS #{schema};|
  end

  def create_view_stmt(table) do
    ~s|
    CREATE TABLE IF NOT EXISTS #{table}
    (
      key VARCHAR PRIMARY KEY,
      collection VARCHAR,
      value JSONB
    );
    |
  end

  def create_events_stmt(view_table, events_table) do
    ~s|
    CREATE TABLE IF NOT EXISTS #{events_table}
    (
      id BIGSERIAL PRIMARY KEY,
      key_id VARCHAR NOT NULL,
      collection VARCHAR,
      type VARCHAR,
      create_ts BIGINT,
      data BYTEA,
      FOREIGN KEY (key_id) REFERENCES #{view_table}(key) ON DELETE CASCADE
    );
    |
  end

  def create_index_stmt(table, column) do
    ~s|CREATE INDEX CONCURRENTLY #{column}_idx ON #{table} (#{column});|
  end

  def prune_count_stmt(table) do
    ~s|SELECT count (id) FROM #{table} WHERE type = $1;|
  end

  def prune_delete_stmt(table) do
    ~s|
    DELETE FROM #{table}
    WHERE id IN
    (
      SELECT id
      FROM #{table}
      WHERE type = $1
      ORDER BY create_ts
      LIMIT $2
    );
    |
  end
end
