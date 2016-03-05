defmodule ConCache.Item do
  @moduledoc """
  This struct can be used in place of naked values to set per-item TTL values.
  """
  defstruct value: nil, ttl: 0
  @type t :: %ConCache.Item{value: ConCache.value, ttl: pos_integer | :renew | :no_update}
end

defmodule ConCache do
  @moduledoc """
  Implements an ETS based key/value storage with following additional features:

  - row level synchronized writes (inserts, read/modify/write updates, deletes)
  - TTL support
  - modification callbacks

  Example usage:

      ConCache.start_link([], name: :my_cache)
      ConCache.put(:my_cache, :foo, 1)
      ConCache.get(:my_cache, :foo)  # 1


  ## Isolation

  ConCache implements additional isolation of write operations. Whenever you
  invoke functions such as `put/3`, or `update/3`, an internal lock is acquired.
  The locking mechanism is powered by the custom sharded implementation. This means
  that there is some overhead compared to standard ETS operations. The gain is
  that you can synchronize more elaborate actions, such as read/modify/write
  operations without the need to pass the data to some synchronizing process.

  If you're certain there's no competition on some operations, you can use dirty
  equivalents, such as `dirty_put/3` or `dirty_update/3`. Dirty operations don't
  acquire the lock, so they boil down to standard ETS isolation properties. Dirty
  operations make sense if:

  - Only one process is modifying the same row at any point in time
  - Only simple write operations are used (`put/3`, `insert_new/3`, `delete/2`)

  In such situations, locks present a needless overhead so you can resort to dirty
  operations.

  Reads are always dirty, i.e. they don't rely on the additional custom locking
  layer. If you need to isolate reads, you can use `isolated/4`. The same function
  can be used to introduce custom isolation level.

  ## Item expiry

  By default items do not expire. You can change this by providing `:ttl` and
  `:ttl_check` options to `start_link/2`. When you configure expiry, items will
  be removed from the cache after some time.

  The `:ttl` option sets the default time-to-live, while `:ttl_check` sets the
  interval of purging. An item can thus live at most `ttl + ttl_check` milliseconds.
  To balance CPU and memory usage, it's advised to set `:ttl_check` to be no less
  than one second.

  It is possible to set per-item TTL. In all update operations, you can provide
  `ConCache` struct as values. The struct contains two fields: `value` with the
  actual value which will be stored, and `ttl` where you can specify item's ttl.

  By default, an item TTL is renewed on every modification, while reads to not
  affect TTL. If you don't want to extend TTL while modifying an item, you can provide
  `%ConCache.Item{value: some_value, ttl: :no_update}` as the value. If you want
  to renew TTL on read, you can provide `:touch_on_read` option to `start_link/2`.

  Finally, you can manually renew the item's TTL with `touch/2`.

  Expiry algorithm is not consistent. It is implemented concurrently, to reduce the
  CPU usage, and works on a "best-effort" basis. It should usually work as expected,
  but some race conditions might occur. For example, in some rare situations it is
  possible that you update an item, and it expires in the next moment. This happens
  because expiry process runs independently from readers/writers. Consequently, you
  shouldn't expect strong guarantees from the expiry mechanism. It is devised for
  caches where items must be purged after they're not used for some time.

  The expiry algorithm is not a brute force scan. It works in discrete ticks, so
  it knows exactly which items must expire at every tick. This reduces the
  amount of work needed to be done at each step, and it's resilient to time shifts.
  However, this approach also increases the memory usage, since a copy of each key
  exists in the expiry process.
  """

  alias ConCache.Owner
  alias ConCache.Operations

  defstruct [
    :owner_pid, :ets, :ttl_check, :ttl, :acquire_lock_timeout, :callback, :touch_on_read
  ]

  @type t :: pid | atom | {:global, any} | {:via, atom, any}

  @type key :: any
  @type value :: any
  @type store_value :: value | ConCache.Item.t

  @type callback_fun :: (({:update, pid, key, value} | {:delete, pid, key}) -> any)

  @type ets_option ::
    :named_table | :compressed | {:heir, pid} |
    {:write_concurrency, boolean} | {:read_concurrency, boolean} |
    :ordered_set | :set | {:name, atom}

  @type options :: [
    {:ttl, non_neg_integer} |
    {:acquire_lock_timeout, pos_integer} |
    {:callback, callback_fun} |
    {:touch_on_read, boolean} |
    {:ttl_check, non_neg_integer} |
    {:time_size, pos_integer} |
    {:ets_options, [ets_option]}
  ]

  @type update_fun :: ((value) -> {:ok, store_value} | {:error, any})

  @type store_fun :: (() -> store_value)

  @doc """
  Starts the server and creates an ETS table.

  Options:
    - `:set` - An ETS table will be of the `:set` type (default).
    - `:ordered_set` - An ETS table will be of the `:ordered_set` type.
    - `{:ttl_check, time_ms}` - A check interval for TTL expiry. This value is
      by default `nil` and you need to provide a positive integer for TTL to work.
      See below for more details on inner workings of TTL.
    - `{:ttl, time_ms}` - The default time after which an item expires.
      When an item expires, it is removed from the cache. Updating the item
      extends its expiry time. By default, items never expire.
    - `{:touch_on_read, true | false}` - Controls whether read operation extends
      expiry of items. False by default.
    - `{:callback, callback_fun}` - If provided, this function is invoked __after__
      an item is inserted or updated, or __before__ it is deleted.
    - `{:acquire_lock_timeout, timeout_ms}` - The time a client process waits for
      the lock. Default is 5000.

  In addition, following ETS options are supported:
    - `:named_table`
    - `:name`
    - `:heir`
    - `:write_concurrency`
    - `:read_concurrency`

  ## Choosing ttl_check time

  When TTL is configured, the owner process works in discrete steps, doing
  cleanups every `ttl_check_time` milliseconds. This approach allows the owner
  process to do fairly small amount of work in each discrete step.

  Assuming there's no huge system overload, an item's max lifetime is thus
  `ttl_time + ttl_check_time` [ms], after the last item's update.

  Thus, lower value of ttl_check time means more frequent purging which may
  reduce your memory consumption, but could also cause performance penalties.
  Higher values put less pressure on processing, but item expiry is less precise.
  """
  @spec start_link(options, GenServer.options) :: GenServer.on_start
  def start_link(options \\ [], gen_server_options \\ []) do
    Owner.start_link(options, gen_server_options)
  end

  @doc """
  Starts the server.

  See `start_link/2` for more details.
  """
  @spec start(options, GenServer.options) :: GenServer.on_start
  def start(options \\ [], gen_server_options \\ []) do
    Owner.start(options, gen_server_options)
  end

  @doc """
  Returns the ets table managed by the cache.
  """
  @spec ets(t) :: :ets.tab
  def ets(cache_id), do: Operations.ets(Owner.cache(cache_id))

  @doc """
  Reads the item from the cache.

  A read is always "dirty", meaning it doesn't block while someone is updating
  the item under the same key. A read doesn't expire TTL of the item, unless
  `touch_on_read` option is set while starting the cache.
  """
  @spec get(t, key) :: value
  def get(cache_id, key), do: Operations.get(Owner.cache(cache_id), key)

  @doc """
  Stores the item into the cache.
  """
  @spec put(t, key, store_value) :: :ok
  def put(cache_id, key, value),
    do: Operations.put(Owner.cache(cache_id), key, value)

  @doc """
  Returns the number of items stored in the cache.
  """
  @spec size(t) :: non_neg_integer
  def size(cache_id),
    do: Operations.size(Owner.cache(cache_id))

  @doc """
  Dirty equivalent of `put/3`.
  """
  @spec dirty_put(t, key, store_value) :: :ok
  def dirty_put(cache_id, key, value),
    do: Operations.dirty_put(Owner.cache(cache_id), key, value)

  @doc """
  Inserts the item into the cache unless it exists.
  """
  @spec insert_new(t, key, store_value) :: :ok | {:error, :already_exists}
  def insert_new(cache_id, key, value),
    do: Operations.insert_new(Owner.cache(cache_id), key, value)

  @doc """
  Dirty equivalent of `insert_new/3`.
  """
  @spec dirty_insert_new(t, key, store_value) :: :ok | {:error, :already_exists}
  def dirty_insert_new(cache_id, key, value),
    do: Operations.insert_new(Owner.cache(cache_id), key, value)

  @doc """
  Updates the item, or stores new item if it doesn't exist.

  The `update_fun` is invoked after the item is locked. Here, you can be certain
  that no other process will update this item, unless they are doing dirty updates
  or writing directly to the underlying ETS table.

  The updater lambda must return one of the following:

    - `{:ok, value}` - causes the value to be stored into the table

  """
  @spec update(t, key, update_fun) :: :ok | {:error, any}
  def update(cache_id, key, update_fun),
    do: Operations.update(Owner.cache(cache_id), key, update_fun)

  @doc """
  Dirty equivalent of `update/3`.
  """
  @spec dirty_update(t, key, update_fun) :: :ok | {:error, any}
  def dirty_update(cache_id, key, update_fun),
    do: Operations.dirty_update(Owner.cache(cache_id), key, update_fun)

  @doc """
  Updates the item only if it exists. Otherwise works just like `update/3`.
  """
  @spec update_existing(t, key, update_fun) :: :ok | {:error, :not_existing} | {:error, any}
  def update_existing(cache_id, key, update_fun),
    do: Operations.update_existing(Owner.cache(cache_id), key, update_fun)

  @doc """
  Dirty equivalent of `update_existing/3`.
  """
  @spec dirty_update_existing(t, key, update_fun) :: :ok | {:error, :not_existing} | {:error, any}
  def dirty_update_existing(cache_id, key, update_fun),
    do: Operations.dirty_update_existing(Owner.cache(cache_id), key, update_fun)

  @doc """
  Deletes the item from the cache.
  """
  @spec delete(t, key) :: :ok
  def delete(cache_id, key), do: Operations.delete(Owner.cache(cache_id), key)

  @doc """
  Dirty equivalent of `delete/2`.
  """
  @spec dirty_delete(t, key) :: :ok
  def dirty_delete(cache_id, key), do: Operations.dirty_delete(Owner.cache(cache_id), key)

  @doc """
  Retrieves the item from the cache, or inserts the new item.

  If the item exists in the cache, it is retrieved. Otherwise, the lambda
  function is executed and its result is stored under the given key.

  Note: if the item is already in the cache, this function amounts to a simple get
  without any locking, so you can expect it to be fairly fast.
  """
  @spec get_or_store(t, key, store_fun) :: value
  def get_or_store(cache_id, key, store_fun),
    do: Operations.get_or_store(Owner.cache(cache_id), key, store_fun)

  @doc """
  Dirty equivalent of `get_or_store/3`.
  """
  @spec dirty_get_or_store(t, key, store_fun) :: value
  def dirty_get_or_store(cache_id, key, store_fun),
    do: Operations.dirty_get_or_store(Owner.cache(cache_id), key, store_fun)

  @doc """
  Manually touches the item to prolongate its expiry.
  """
  @spec touch(t, key) :: :ok
  def touch(cache_id, key), do: Operations.touch(Owner.cache(cache_id), key)

  @doc """
  Isolated execution over arbitrary lock in the cache.

  You can do whatever you want in the function, not necessarily related to the
  cache. The return value is the result of the provided lambda.

  This allows you to perform flexible isolation. If you use the key
  of your item as a `key`, then this operation will be exclusive to
  updates. This can be used e.g. to perform isolated reads:

      # Process A:
      ConCache.isolated(:my_cache, :my_item_key, fn() -> ... end)

      # Process B:
      ConCache.update(:my_cache, :my_item_key, fn(old_value) -> ... end)

  These two operations are mutually exclusive.
  """
  @spec isolated(t, key, nil | pos_integer, (() -> any)) :: any
  def isolated(cache_id, key, timeout \\ nil, fun),
    do: Operations.isolated(Owner.cache(cache_id), key, timeout, fun)

  @doc """
  Similar to `isolated/4` except it doesn't wait for the lock to be available.

  If the lock can be acquired immediately, it will be acquired and the function
  will be invoked. Otherwise, an error is returned immediately.
  """
  @spec try_isolated(t, key, nil | pos_integer, (() -> any)) :: {:error, :locked} | {:ok, any}
  def try_isolated(cache_id, key, timeout \\ nil, on_success),
    do: Operations.try_isolated(Owner.cache(cache_id), key, timeout, on_success)
end
