defmodule BrookStoragePostgres.StorageTest do
  use ExUnit.Case
  use Divo

  alias BrookStoragePostgres.Storage, as: Postgres

  @instance :postgres_test
  @postgrex_args [hostname: "localhost", username: "brook", password: "brook", database: "view_test"]

  describe "persist/4" do
    setup [:start_storage_postgres, :start_validation_postgres]

    test "will save the key/value in a collection", %{postgres: postgres} do
      event = Brook.Event.new(type: "create", author: "testing", data: "data")
      Postgres.persist(@instance, event, "people", "key1", %{"one" => 1})

      saved_value =
        Postgrex.query!(postgres, "select data from state", [])
        |> (fn %{rows: [[rows]]} -> rows end).()
        |> Brook.Deserializer.deserialize()

      assert %{"one" => 1} == saved_value
    end

    test "will append the event to the events table", %{postgres: postgres} do
      event1 = Brook.Event.new(author: "bob", type: "create", data: %{"one" => 1})
      event2 = Brook.Event.new(author: "bob", type: "update", data: %{"one" => 1, "two" => 2})

      :ok = Postgres.persist(@instance, event1, "people", "key1", event1.data)
      :ok = Postgres.persist(@instance, event2, "people", "key1", event2.data)

      saved_value =
        postgres
        |> Postgrex.query!("select data from state", [])
        |> (fn %{rows: [[rows]]} -> rows end).()
        |> Brook.Deserializer.deserialize()
        |> (fn {:ok, decoded_value} -> decoded_value end).()

      assert %{"one" => 1, "two" => 2} == saved_value

      create_event_list =
        postgres
        |> Postgrex.query!("select data from events where type=create", [])
        |> (fn %{rows: [[rows]]} -> rows end).()
        |> Brook.Deserializer.deserialize()
        |> (fn {:ok, decoded_event} -> decoded_event end).()

      assert [event1] == create_event_list

      update_event_list =
        postgres
        |> Postgrex.query!("select data from events where type=update", [])
        |> (fn %{rows: [[rows]]} -> rows end).()
        |> Brook.Deserializer.deserialize()
        |> (fn {:ok, decoded_event} -> decoded_event end).()

      assert [event2] == update_event_list
    end

    test "will return an error tuple when postgres has an error" do
    end

    test "will only save configured number for event with restrictions" do
      Enum.each(1..10, fn i ->
        create_event = Brook.Event.new(type: "create", author: "testing", data: i)
        Postgres.persist(@instance)
      end)
    end
  end

  defp start_storage_postgres(_context) do
    registry_name = Brook.Config.registry(@brook_instance)
    {:ok, registry} = Registry.start_link(name: registry_name, keys: :unique)

    {:ok, postgres} =
      Postgres.start_link(
        instance: @instance,
        postgrex_args: @postgrex_args,
        event_limits: %{"restricted" => 3}
      )

    on_exit(fn ->
      kill(postgres)
      kill(registry)
    end)

    :ok
  end

  defp start_validation_postgres(_context) do
    {:ok, postgres} =
      Postgrex.start_link(@postgrex_args)

    Postgrex.transaction(postgres, fn postgres ->
      Postgrex.query!(postgres, "drop table events", [])
      Postgrex.query!(postgres, "drop table state", [])
    end)

    on_exit(fn -> kill(postgres) end)

    [postgres: postgres]
  end

  defp kill(pid) do
    ref = Process.monitor(pid)
    Process.exit(ref, :normal)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
