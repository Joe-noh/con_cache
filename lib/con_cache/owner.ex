defmodule ConCache.Owner do
  @moduledoc false

  use ExActor.Tolerant
  use Bitwise

  alias ConCache.Expiry

  defstruct [
    ttl_check: nil,
    monitor_ref: nil,
    expiry: nil
  ]

  def cache({:local, local}) when is_atom(local), do: cache(local)
  def cache(local) when is_atom(local), do: ConCache.Registry.get(Process.whereis(local))
  def cache({:global, name}), do: cache({:via, :global, name})
  def cache({:via, module, name}), do: cache(module.whereis_name(name))
  def cache(pid) when is_pid(pid), do: ConCache.Registry.get(pid)

  defstart start_link(options \\ []), gen_server_opts: :runtime
  defstart start(options \\ []), gen_server_opts: :runtime do
    ets = create_ets(options[:ets_options] || [])
    check_ets(ets)

    cache = %ConCache{
      owner_pid: self,
      ets: ets,
      ttl: options[:ttl] || 0,
      acquire_lock_timeout: options[:acquire_lock_timeout] || 5000,
      callback: options[:callback],
      touch_on_read: options[:touch_on_read] || false
    }

    state = init_ttl_check(options)
    if Map.get(state, :ttl_check) != nil do
      cache = %ConCache{cache | ttl_manager: self}
    end

    state = %__MODULE__{state | monitor_ref: Process.monitor(Process.whereis(:con_cache_registry))}
    ConCache.Registry.register(cache)

    initial_state(state)
  end

  defp create_ets(input_options) do
    %{name: name, type: type, options: options} = parse_ets_options(input_options)
    :ets.new(name, [type | options])
  end

  defp parse_ets_options(input_options) do
    Enum.reduce(
      input_options,
      %{name: :con_cache, type: :set, options: [:public]},
        fn
          (:named_table, acc) -> append_option(acc, :named_table)
          (:compressed, acc) -> append_option(acc, :compressed)
          ({:heir, _} = opt, acc) -> append_option(acc, opt)
          ({:write_concurrency, _} = opt, acc) -> append_option(acc, opt)
          ({:read_concurrency, _} = opt, acc) -> append_option(acc, opt)
          (:ordered_set, acc) -> %{acc | type: :ordered_set}
          (:set, acc) -> %{acc | type: :set}
          ({:name, name}, acc) -> %{acc | name: name}
          (other, _) -> throw({:invalid_ets_option, other})
        end
    )
  end

  defp append_option(%{options: options} = ets_options, option) do
    %{ets_options | options: [option | options]}
  end

  defp check_ets(ets) do
    if (:ets.info(ets, :keypos) > 1), do: throw({:error, :invalid_keypos})
    if (:ets.info(ets, :protection) != :public), do: throw({:error, :invalid_protection})
    if (not (:ets.info(ets, :type) in [:set, :ordered_set])), do: throw({:error, :invalid_type})
  end

  defp init_ttl_check(options) do
    case options[:ttl_check] do
      ttl_check when is_integer(ttl_check) and ttl_check > 0 ->
        queue_expiry(ttl_check)
        %__MODULE__{
          ttl_check: ttl_check,
          expiry: Expiry.new((1 <<< (options[:time_size] || 64)) - 1),
        }

      _ -> %__MODULE__{ttl_check: nil}
    end
  end


  def clear_ttl(server, key) do
    set_ttl(server, key, 0)
  end

  defcast set_ttl(key, ttl), state: state do
    %__MODULE__{state | expiry: Expiry.set(state.expiry, key, expires_after(state.ttl_check, ttl))}
    |> new_state
  end

  defhandleinfo :run_expiry, state: state do
    state
    |> run_expiry
    |> new_state
  end

  defhandleinfo {:DOWN, ref1, _, _, reason},
    state: %__MODULE__{monitor_ref: ref2},
    when: ref1 == ref2,
    do: stop_server(reason)

  defhandleinfo _, do: noreply


  defp run_expiry(state) do
    {expired, expiry} = Expiry.next_step(state.expiry)
    Enum.each(expired, &ConCache.delete(self, &1))

    queue_expiry(state.ttl_check)
    %__MODULE__{state | expiry: expiry}
  end


  defp queue_expiry(ttl_check) do
    Process.send_after(self, :run_expiry, ttl_check)
  end

  defp expires_after(_, :renew), do: :renew
  defp expires_after(ttl_check, ttl) do
    steps = ttl / ttl_check
    isteps = trunc(steps)
    if steps > isteps do
      isteps + 1
    else
      isteps
    end
  end

  if Mix.env == :test do
    defcall expire, state: state do
      set_and_reply(run_expiry(state), :ok)
    end
  end
end
