defmodule ConCache.Expiry do
  @moduledoc false

  defstruct [
    current_step: 0,
    expiry_items: %{},  # step -> items to expire
    items_expiry: %{},  # item -> {expires_at, expires_after}
    pending: %{},       # item -> expires_at (pending expires at the next step)
    max_step: nil
  ]

  def new(max_step \\ :infinity) do
    %__MODULE__{max_step: max_step}
  end

  def set(state, item, expires_after) do
    %__MODULE__{state |
      pending: Map.update(state.pending, item, expires_after, &new_expires_after(&1, expires_after))
    }
  end

  defp new_expires_after(existing, :renew), do: existing
  defp new_expires_after(_, new_expires_after), do: new_expires_after


  def next_step(state) do
    next_state =
      state
      |> shift_step
      |> apply_pending_expires

    expired = Map.get(next_state.expiry_items, next_state.current_step, MapSet.new)

    {
      expired,
      %__MODULE__{next_state |
        items_expiry: Enum.reduce(expired, next_state.items_expiry, &Map.delete(&2, &1)),
        expiry_items: Map.delete(next_state.expiry_items, next_state.current_step)
      }
    }
  end

  defp apply_pending_expires(state) do
    state =
      Enum.reduce(state.pending, state,
        fn({item, expires_after}, state) -> set_expiry(state, item, expires_after) end
      )

    %__MODULE__{state | pending: %{}}
  end

  defp set_expiry(state, item, :renew) do
    case Map.fetch(state.items_expiry, item) do
      {:ok, {_, expires_after}} -> set_expiry(state, item, expires_after)
      :error ->
        # Not an error because of concurrency. It's possible the client wants to
        # renew, but the item has expired in the meantime. In this case, we
        # just leave the state as it is.
        state
    end
  end

  defp set_expiry(state, item, expires_after) do
    state
    |> remove_previous_expiry(item)
    |> store_new_expiry(item, expires_after)
  end


  defp remove_previous_expiry(state, item) do
    case Map.fetch(state.items_expiry, item) do
      :error -> state
      {:ok, {expires_at, _}} ->
        %__MODULE__{state |
          expiry_items: Map.update!(state.expiry_items, expires_at, &MapSet.delete(&1, item))
        }
    end
  end

  defp store_new_expiry(state, _, 0), do: state
  defp store_new_expiry(state, item, expires_after) when(is_integer(expires_after) and expires_after > 0) do
    expires_at = state.current_step + expires_after

    %__MODULE__{state |
      items_expiry: Map.put(state.items_expiry, item, {expires_at, expires_after}),
      expiry_items: Map.update(state.expiry_items, expires_at, MapSet.new([item]), &MapSet.put(&1, item))
    }
  end


  defp shift_step(%__MODULE__{current_step: max_step, max_step: max_step} = state) do
    %__MODULE__{(normalize_steps(state)) | current_step: 0}
  end

  defp shift_step(state) do
    %__MODULE__{state | current_step: state.current_step + 1}
  end


  defp normalize_steps(state) do
    %__MODULE__{state |
      expiry_items:
        state.expiry_items
        |> Stream.map(
              fn({expires_at, items}) ->
                {expires_at - state.current_step - 1, items}
              end
            )
        |> Enum.into(%{}),

      items_expiry:
        state.items_expiry
        |> Stream.map(
              fn({item, {expires_at, expires_after}}) ->
                {item, {expires_at - state.current_step - 1, expires_after}}
              end
            )
        |> Enum.into(%{})
    }
  end
end
