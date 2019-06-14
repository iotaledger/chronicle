defmodule ExtendedApi.Worker.FindTransactions.Hints.ZeroValueFn do

  @moduledoc """
    This module hold the functions that are going to compute
    the zero_value query result, it creates one query per
    address's bundle_hash to fetch tx-hashes.

    finally it returns
    queries_state_list. or {:error, reason}
  """
  alias ExtendedApi.Worker.FindTransactions.Hints.Helper
  @doc """
    it takes row, and acc(queries_states_list) initial is empty list.
  """
  def bundle_queries([ts: ts, v2: v2, ix: ix, el: el], acc) when is_list(acc) do
    # lb = 10, we already know it's an output as the value = 0.
    # ts indicates bundle_timestamp
    # v2 is bundle-hash
    # ix is current index
    # el is extra label which indicates the transaction hash
    # is headhash or txhash.
    # acc is simply a list.
    # we are sending the bundle query.
    {ok?, {_, qf}, query_state} =
      Helper.bundle_query(v2, el, ts, ix)
    if ok? == :ok do
      # we put the query_state in queries_states map.
      # we return updated acc
      [{qf, query_state} | acc]
    else
      # we break.
      {:error, {:dead_shard_stage, ok?} }
    end
  end

  @doc """
   This function handle when acc is an error.
   {:error, {:dead_shard_stage, ok?}}.
   it just return acc to break the api call.
  """
  @spec bundle_queries(binary, Keyword.t, {:error, tuple}) :: tuple
  def bundle_queries(_, _, acc) do
    # we keep breaking.
    acc
  end

end
