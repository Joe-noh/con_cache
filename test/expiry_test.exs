defmodule ExpiryTest do
  use ExUnit.Case, async: true

  alias ConCache.Expiry

  test "empty" do
    step_and_verify(Expiry.new, [])
  end

  test "basic expiry" do
    Expiry.new
    |> Expiry.set(:foo, 1)
    |> step_and_verify([])
    |> step_and_verify([:foo])
  end

  test "prolonging expiry" do
    Expiry.new
    |> Expiry.set(:foo, 1)
    |> step_and_verify([])
    |> Expiry.set(:foo, 1)
    |> step_and_verify([])
    |> step_and_verify([:foo])
  end

  test "multiple expiries of the same item in the same step" do
    Expiry.new
    |> Expiry.set(:foo, 100)
    |> Expiry.set(:foo, 1)
    |> step_and_verify([])
    |> step_and_verify([:foo])
  end

  test "expiring different items" do
    Expiry.new
    |> Expiry.set(:foo, 1)
    |> Expiry.set(:bar, 2)
    |> Expiry.set(:baz, 2)
    |> step_and_verify([])
    |> step_and_verify([:foo])
    |> step_and_verify([:bar, :baz])
  end

  test "zero expiry is ignored" do
    Expiry.new
    |> Expiry.set(:foo, 1)
    |> Expiry.set(:foo, 0)
    |> step_and_verify([])
    |> step_and_verify([])
  end

  test "renewal" do
    Expiry.new
    |> Expiry.set(:foo, 1)
    |> step_and_verify([])
    |> Expiry.set(:foo, :renew)
    |> step_and_verify([])
    |> step_and_verify([:foo])
  end

  test "renewing an expired item" do
    Expiry.new
    |> Expiry.set(:foo, 1)
    |> step_and_verify([])
    |> step_and_verify([:foo])
    |> Expiry.set(:foo, :renew)
    |> step_and_verify([])
  end

  test "max time normalization" do
    Expiry.new(3)
    |> Expiry.set(:foo, 1)
    |> Expiry.set(:bar, 4)
    |> step_and_verify([])
    |> step_and_verify([:foo])
    |> step_and_verify([])
    |> Expiry.set(:foo, 1)
    |> step_and_verify([])
    |> step_and_verify([:foo, :bar])
  end

  defp step_and_verify(expiry, expected_removes) do
    {removed_items, expiry} = Expiry.next_step(expiry)
    assert Enum.sort(expected_removes) == Enum.sort(removed_items)
    expiry
  end
end
