defmodule Brook.Storage.PostgresTest do
  use ExUnit.Case
  use Divo
  use Placebo

  alias Brook.Storage.Postgres

  @instance :postgres_test
  @table "brook_test"
  @postgrex_args [
    hostname: "localhost",
    username: "brook",
    password: "brook",
    database: "view_test"
  ]

  describe "persist/4" do
    setup [:start_validation_postgres, :start_storage_postgres]

    test "will save the key/value in a collection", %{postgres: postgres} do
      event = Brook.Event.new(type: "create", author: "testing", data: "data")
      Postgres.persist(@instance, event, "people", "key1", %{"one" => 1})

      {:ok, saved_value} =
        Postgrex.query!(postgres, "SELECT value FROM #{@table};", [])
        |> (fn %{rows: [[rows]]} -> rows end).()
        |> Map.get("value")
        |> Brook.Deserializer.deserialize()

      assert %{"one" => 1} == saved_value
    end

    test "allows unique combinations of collection and key", %{postgres: postgres} do
      event1 = Brook.Event.new(type: "create", author: "testing", data: %{"a" => 1})
      event2 = Brook.Event.new(type: "create", author: "testing", data: %{"b" => 2})

      :ok = Postgres.persist(@instance, event1, "collection-1", "key", event1.data)
      :ok = Postgres.persist(@instance, event2, "collection-2", "key", event2.data)

      saved_values =
        postgres
        |> Postgrex.query!("SELECT value FROM #{@table}", [])
        |> (fn result -> List.flatten(result.rows) end).()
        |> Enum.map(&Map.get(&1, "value"))
        |> Enum.map(&Brook.Deserializer.deserialize/1)
        |> Enum.map(&elem(&1, 1))

      assert [%{"a" => 1}, %{"b" => 2}] == saved_values
    end

    test "will append the event to the events table", %{postgres: postgres} do
      event1 = Brook.Event.new(author: "bob", type: "create", data: %{"one" => 1})
      event2 = Brook.Event.new(author: "bob", type: "update", data: %{"one" => 1, "two" => 2})

      :ok = Postgres.persist(@instance, event1, "people", "key1", event1.data)
      :ok = Postgres.persist(@instance, event2, "people", "key1", event2.data)

      saved_value =
        postgres
        |> Postgrex.query!("SELECT value FROM #{@table}", [])
        |> (fn %{rows: [[rows]]} -> rows end).()
        |> Map.get("value")
        |> Brook.Deserializer.deserialize()
        |> (fn {:ok, decoded_value} -> decoded_value end).()

      assert %{"one" => 1, "two" => 2} == saved_value

      [{:ok, create_event}] =
        postgres
        |> Postgrex.query!("SELECT data FROM #{@table}_events WHERE type = $1", ["create"])
        |> (fn %{rows: [rows]} -> rows end).()
        |> Enum.map(&:zlib.gunzip/1)
        |> Enum.map(&Brook.Deserializer.deserialize/1)

      assert event1 == create_event

      [{:ok, update_event}] =
        postgres
        |> Postgrex.query!("SELECT data FROM #{@table}_events WHERE type = $1", ["update"])
        |> (fn %{rows: [rows]} -> rows end).()
        |> Enum.map(&:zlib.gunzip/1)
        |> Enum.map(&Brook.Deserializer.deserialize/1)

      assert event2 == update_event
    end

    test "will return an error tuple when postgres returns an error" do
      allow(Postgrex.query(any(), any(), any()), return: {:error, :failure_struct})

      event = Brook.Event.new(type: "create", author: "testing", data: "data")

      assert {:error, :failure_struct} ==
               Postgres.persist(@instance, event, "people", "key1", %{one: 1})
    end

    test "will only save configured number for event with restrictions" do
      Enum.each(1..10, fn i ->
        create_event = Brook.Event.new(type: "create", author: "testing", data: i)
        Postgres.persist(@instance, create_event, "people", "key4", %{"name" => "joe"})
        restricted_event = Brook.Event.new(type: "restricted", author: "testing", data: i)
        Postgres.persist(@instance, restricted_event, "people", "key4", %{"name" => "joe"})
      end)

      Process.sleep(100)

      {:ok, events} = Postgres.get_events(@instance, "people", "key4")
      grouped_events = Enum.group_by(events, fn event -> event.type end)

      assert 10 == length(grouped_events["create"])
      assert 5 == length(grouped_events["restricted"])
      assert [6, 7, 8, 9, 10] == Enum.map(grouped_events["restricted"], fn x -> x.data end)
    end
  end

  describe "get/2" do
    setup [:start_validation_postgres, :start_storage_postgres]

    test "will return the value persisted to postgres" do
      event = Brook.Event.new(type: "create", author: "testing", data: :data1)
      :ok = Postgres.persist(@instance, event, "people", "key1", %{name: "joe"})

      assert {:ok, %{"name" => "joe"}} == Postgres.get(@instance, "people", "key1")
    end

    test "returns an error tuple when postgrex returns an error" do
      allow(Postgrex.query(any(), any(), any()), return: {:error, :failure_struct})

      assert {:error, :failure_struct} == Postgres.get(@instance, "people", "key1")
    end
  end

  describe "get_events/2" do
    setup [:start_validation_postgres, :start_storage_postgres]

    test "returns all events for key" do
      event1 = Brook.Event.new(author: "steve", type: "create", data: %{"one" => 1}, create_ts: 0)

      event2 =
        Brook.Event.new(
          author: "steve",
          type: "update",
          data: %{"one" => 1, "two" => 2},
          create_ts: 1
        )

      :ok = Postgres.persist(@instance, event1, "people", "key1", event1.data)
      :ok = Postgres.persist(@instance, event2, "people", "key1", event2.data)

      assert {:ok, [event1, event2]} == Postgres.get_events(@instance, "people", "key1")
    end

    test "returns only events matching type" do
      event1 = Brook.Event.new(author: "steve", type: "create", data: %{"one" => 1}, create_ts: 0)

      event2 =
        Brook.Event.new(
          author: "steve",
          type: "update",
          data: %{"one" => 1, "two" => 2},
          create_ts: 1
        )

      event3 = Brook.Event.new(author: "steve", type: "create", data: %{"one" => 1}, create_ts: 2)

      :ok = Postgres.persist(@instance, event1, "people", "key1", event1.data)
      :ok = Postgres.persist(@instance, event2, "people", "key1", event2.data)
      :ok = Postgres.persist(@instance, event3, "people", "key1", event3.data)

      assert {:ok, [event1, event3]} == Postgres.get_events(@instance, "people", "key1", "create")
    end

    test "returns error tuple when postgrex returns an error" do
      allow(Postgrex.query(any(), any(), any()), return: {:error, :failure_struct})

      assert {:error, :failure_struct} == Postgres.get_events(@instance, "people", "key1")
    end
  end

  describe "get_all/1" do
    setup [:start_validation_postgres, :start_storage_postgres]

    test "returns all the values in a collection" do
      event = Brook.Event.new(type: "create", author: "testing", data: "data")
      :ok = Postgres.persist(@instance, event, "people", "key1", "value1")
      :ok = Postgres.persist(@instance, event, "people", "key2", "value2")
      :ok = Postgres.persist(@instance, event, "people", "key3", "value3")

      expected = %{"key1" => "value1", "key2" => "value2", "key3" => "value3"}

      assert {:ok, expected} == Postgres.get_all(@instance, "people")
    end

    test "returns error tuple when postgrex returns an error" do
      allow(Postgrex.query(any(), any(), any()), return: {:error, :failure_struct})

      assert {:error, :failure_struct} == Postgres.get_all(@instance, "people")
    end

    test "returns empty map when no data available" do
      assert {:ok, %{}} == Postgres.get_all(@instance, "jerks")
    end
  end

  describe "delete/2" do
    setup [:start_validation_postgres, :start_storage_postgres]

    test "deletes view and event entries in postgres" do
      event = Brook.Event.new(type: "create", author: "testing", data: "data1")
      :ok = Postgres.persist(@instance, event, "people", "key1", "value1")
      assert {:ok, "value1"} == Postgres.get(@instance, "people", "key1")

      :ok = Postgres.delete(@instance, "people", "key1")
      assert :ok = Postgres.delete(@instance, "people", "key1")
      assert {:ok, nil} == Postgres.get(@instance, "people", "key1")
      assert {:ok, []} == Postgres.get_events(@instance, "people", "key1")
    end

    test "returns error tuple when postgrex returns error tuple" do
      allow(Postgrex.query(any(), any(), any()), return: {:error, :failure_struct})

      result = Postgres.delete(@instance, "people", "key2")
      assert {:error, :failure_struct} == result
    end
  end

  defp start_storage_postgres(%{postgres: postgres}) do
    registry_name = Brook.Config.registry(@instance)
    start_supervised({Registry, name: registry_name, keys: :unique})

    start_supervised(
      {Postgres,
       instance: @instance,
       table: @table,
       postgrex_args: @postgrex_args,
       event_limits: %{"restricted" => 5}}
    )

    db_ready?(postgres)
    :ok
  end

  defp start_validation_postgres(_context) do
    {:ok, postgres} = start_supervised({Postgrex, @postgrex_args})

    Postgrex.transaction(postgres, fn postgres ->
      Postgrex.query!(postgres, "DROP TABLE IF EXISTS #{@table}_events;", [])
      Postgrex.query!(postgres, "DROP TABLE IF EXISTS #{@table};", [])
    end)

    [postgres: postgres]
  end

  defp db_ready?(conn) do
    case Postgrex.query(conn, "SELECT * FROM #{@table}_events;", []) do
      {:ok, _} ->
        :ok

      {:error, _} ->
        Process.sleep(10)
        db_ready?(conn)
    end
  end
end
