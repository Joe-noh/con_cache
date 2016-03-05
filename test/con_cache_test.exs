defmodule ConCacheTest do
  use ExUnit.Case, async: true

  test "initial" do
    {:ok, cache} = ConCache.start
    assert ConCache.get(cache, :a) == nil
  end

  test "put" do
    {:ok, cache} = ConCache.start
    assert ConCache.put(cache, :a, 1) == :ok
    assert ConCache.get(cache, :a) == 1
  end

  test "insert_new" do
    {:ok, cache} = ConCache.start
    assert ConCache.insert_new(cache, :b, 2) == :ok
    assert ConCache.get(cache, :b) == 2
    assert ConCache.insert_new(cache, :b, 3) == {:error, :already_exists}
    assert ConCache.get(cache, :b) == 2
  end

  test "delete" do
    {:ok, cache} = ConCache.start
    ConCache.put(cache, :a, 1)
    assert ConCache.delete(cache, :a) == :ok
    assert ConCache.get(cache, :a) == nil
  end

  test "update" do
    {:ok, cache} = ConCache.start
    ConCache.put(cache, :a, 1)
    assert ConCache.update(cache, :a, &({:ok, &1 + 1})) == :ok
    assert ConCache.get(cache, :a) == 2

    assert ConCache.update(cache, :a, fn(_) -> {:error, false} end) == {:error, false}
  end

  test "update_existing" do
    {:ok, cache} = ConCache.start
    assert ConCache.update_existing(cache, :a, &({:ok, &1 + 1})) == {:error, :not_existing}
    ConCache.put(cache, :a, 1)
    assert ConCache.update_existing(cache, :a, &({:ok, &1 + 1})) == :ok
    assert ConCache.get(cache, :a) == 2
  end

  test "invalid update" do
    {:ok, cache} = ConCache.start
    ConCache.put(cache, :a, 1)
    assert_raise(
      RuntimeError,
      ~r/^Invalid return value.*/,
      fn -> ConCache.update(cache, :a, fn(_) -> :invalid_return_value end) end
    )
  end

  test "get_or_store" do
    {:ok, cache} = ConCache.start
    assert ConCache.get_or_store(cache, :a, fn() -> 1 end) == 1
    assert ConCache.get_or_store(cache, :a, fn() -> 2 end) == 1
    assert ConCache.get_or_store(cache, :b, fn() -> 4 end) == 4
  end

  test "size" do
    {:ok, cache} = ConCache.start
    assert ConCache.size(cache) == 0
    ConCache.put(cache, :a, "foo")
    assert ConCache.size(cache) == 1
  end

  test "dirty" do
    {:ok, cache} = ConCache.start
    assert ConCache.dirty_put(cache, :a, 1) == :ok
    assert ConCache.get(cache, :a) == 1

    assert ConCache.dirty_insert_new(cache, :b, 2) == :ok
    assert ConCache.get(cache, :b) == 2
    assert ConCache.dirty_insert_new(cache, :b, 3) == {:error, :already_exists}
    assert ConCache.get(cache, :b) == 2
    assert ConCache.dirty_delete(cache, :b) == :ok
    assert ConCache.get(cache, :b) == nil

    assert ConCache.dirty_update(cache, :a, &({:ok, &1 + 1})) == :ok
    assert ConCache.get(cache, :a) == 2

    assert ConCache.dirty_update_existing(cache, :a, &({:ok, &1 + 1})) == :ok
    assert ConCache.get(cache, :a) == 3

    assert ConCache.dirty_update_existing(cache, :b, &({:ok, &1 + 1})) == {:error, :not_existing}
    assert ConCache.get(cache, :b) == nil

    assert ConCache.dirty_get_or_store(cache, :a, fn() -> :dummy end) == 3
    assert ConCache.dirty_get_or_store(cache, :b, fn() -> 4 end) == 4
    assert ConCache.get(cache, :b) == 4
  end

  test "ets_options" do
    {:ok, cache} = ConCache.start(ets_options: [:named_table, {:name, :test_name}])
    assert :ets.info(ConCache.ets(cache), :named_table) == true
    assert :ets.info(ConCache.ets(cache), :name) == :test_name
  end

  test "callback" do
    me = self
    {:ok, cache} = ConCache.start(callback: &send(me, &1))
    ConCache.put(cache, :a, 1)
    assert_receive {:update, ^cache, :a, 1}

    ConCache.update(cache, :a, fn(_) -> {:ok, 2} end)
    assert_receive {:update, ^cache, :a, 2}

    ConCache.update_existing(cache, :a, fn(_) -> {:ok, 3} end)
    assert_receive {:update, ^cache, :a, 3}

    ConCache.delete(cache, :a)
    assert_receive {:delete, ^cache, :a}
  end

  test "ttl" do
    {:ok, cache} = ConCache.start([ttl_check: 1000, ttl: 1])
    ConCache.put(cache, :a, 1)
    assert ConCache.get(cache, :a) == 1
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == 1
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == nil
  end

  test "created key with update should have default ttl" do
    {:ok, cache} = ConCache.start([ttl_check: 1000, ttl: 1])
    ConCache.update(cache, :a, fn(_) -> {:ok, 1} end)
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == 1
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == nil
  end

  test "put renews ttl" do
    test_renew_ttl(fn(cache) -> ConCache.put(cache, :a, 1) end)
  end

  test "update renews ttl" do
    test_renew_ttl(fn(cache) -> ConCache.update(cache, :a, &{:ok, &1}) end)
  end

  test "update existing renews ttl" do
    test_renew_ttl(fn(cache) -> ConCache.update_existing(cache, :a, &{:ok, &1}) end)
  end

  test "touch renews ttl" do
    test_renew_ttl(fn(cache) -> ConCache.touch(cache, :a) end)
  end

  defp test_renew_ttl(fun) do
    {:ok, cache} = ConCache.start(ttl_check: 10000, ttl: 1)
    ConCache.put(cache, :a, 1)
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == 1
    fun.(cache)
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == 1
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == nil
  end

  test "no_update" do
    {:ok, cache} = ConCache.start(ttl_check: 1000, ttl: 1)
    ConCache.put(cache, :a, 1)
    ConCache.Owner.expire(cache)
    ConCache.put(cache, :a, %ConCache.Item{value: 2, ttl: :no_update})
    ConCache.update(cache, :a, fn(_old) -> {:ok, %ConCache.Item{value: 3, ttl: :no_update}} end)
    assert ConCache.get(cache, :a) == 3
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == nil
  end

  test "touch_on_read" do
    {:ok, cache} = ConCache.start(ttl_check: 1000, ttl: 1, touch_on_read: true)
    ConCache.put(cache, :a, 1)
    for _ <- 1..10 do
      ConCache.Owner.expire(cache)
      assert ConCache.get(cache, :a) == 1
    end
    ConCache.Owner.expire(cache)
    ConCache.Owner.expire(cache)
    assert ConCache.get(cache, :a) == nil
  end

  test "real ttl" do
    {:ok, cache} = ConCache.start([ttl_check: 10, ttl: 1])
    ConCache.put(cache, :a, 1)
    assert ConCache.get(cache, :a) == 1
    :timer.sleep(50)
    assert ConCache.get(cache, :a) == nil
  end

  test "try_isolated" do
    {:ok, cache} = ConCache.start
    spawn(fn() ->
      ConCache.isolated(cache, :a, fn() -> :timer.sleep(100) end)
    end)

    :timer.sleep(20)
    assert ConCache.try_isolated(cache, :a, fn() -> flunk "error" end) == {:error, :locked}

    :timer.sleep(100)
    assert ConCache.try_isolated(cache, :a, fn() -> :isolated end) == {:ok, :isolated}
  end

  test "nested" do
    {:ok, cache} = ConCache.start
    assert ConCache.isolated(cache, :a, fn() ->
      ConCache.isolated(cache, :b, fn() ->
        ConCache.isolated(cache, :c, fn() -> 1 end)
      end)
    end) == 1

    assert ConCache.isolated(cache, :a, fn() -> 2 end) == 2
  end

  test "multiple" do
    {:ok, cache1} = ConCache.start
    {:ok, cache2} = ConCache.start
    ConCache.put(cache1, :a, 1)
    ConCache.put(cache2, :b, 2)
    assert ConCache.get(cache1, :a) == 1
    assert ConCache.get(cache1, :b) == nil
    assert ConCache.get(cache2, :a) == nil
    assert ConCache.get(cache2, :b) == 2

    spawn(fn -> ConCache.isolated(cache1, :a, fn -> :timer.sleep(:infinity) end) end)
    assert ConCache.isolated(cache2, :a, fn -> :foo end) == :foo
    assert {:timeout, _} = catch_exit(ConCache.isolated(cache1, :a, 50, fn -> :bar end))
  end

  for name <- [:cache, {:local, :cache}, {:global, :cache}, {:via, :global, :cache}] do
    test "registration #{inspect name}" do
      name = unquote(Macro.escape(name))
      ConCache.start([], name: name)
      ConCache.put(name, :a, 1)
      assert ConCache.get(name, :a) == 1
    end
  end
end
