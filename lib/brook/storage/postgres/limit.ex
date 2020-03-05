defmodule Brook.Storage.Postgres.Limit do
  @moduledoc """
  Provides a simple spawned process for cleaning
  up/pruning event lists when event limits are
  defined by the application.
  """

  import Brook.Storage.Postgres.Statement

  def prune(conn, table, type, limit) do
    {:ok, %Postgrex.Result{rows: [[type_count]]}} = Postgrex.query(conn, prune_count_stmt(table), [type])
    if type_count > limit do
      Postgrex.query(conn, prune_delete_stmt(table), [type, (type_count - limit)])
     end
  end
end
