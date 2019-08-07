defmodule Broker.Collector.Distributor.Helper do
  @moduledoc """
    helper function(s) for distributor
  """
  
  # take unique elements from queue

  @doc """
    Take unique elements from queue up to the provided amount
  """
  @spec take_unique(tuple,integer) :: {list, tuple}
  def take_unique(q, amount), do: do_take(q, amount, [], amount)
  defp do_take(q, n, acc, n_acc) when n > 0 do
    case :queue.out(q) do
      {{:value, e}, rest} ->
        do_take(rest, n - 1, [e | acc],n_acc)
      {:empty, q} ->
        {Enum.uniq(acc), q}
    end
  end
  defp do_take(q, _, acc, n_acc) do
    acc = Enum.uniq(acc)
    case length(acc) do
      ^n_acc ->
       {acc, q}
      acc_length ->
        do_take(q, n_acc-acc_length, acc, n_acc)
    end
  end

end
