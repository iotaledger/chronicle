defmodule ExtendedApi.Worker.FindTransactions.Addresses.EdgeFn do

  @moduledoc """
    This module hold the function that are going to compute
    the edge query result, it creates one query per
    address's bundle_hash to fetch tx-hashes only
    when the label is not hint.

    finally it returns
    {:ok, queries_states, hints} mean the queries has been received
    by shard's stage (reporter) and hints(if any) are ready.
    or {:error, reason} if the reporter/shardstage is dead.
  """
  alias ExtendedApi.Worker.FindTransactions.Addresses.Helper

  @doc """
   This function handle the edge row.
  """
  @spec bundle_queries(binary, Keyword.t, tuple) :: tuple
  def bundle_queries(_address, [{:lb, lb}|_] = row, {:ok, queries_states, _} = acc) when lb != 60 do
    # lb indicates label (output, input)....^
    # ts indicates bundle_timestamp when label(lb)
    # in (input, output) and 0 when label(lb) = hint.
    ts = Keyword.get(row, :ts)
    # v2(bh) indicates bundle_hash.
    bh = Keyword.get(row, :v2)
    # ix indicates current_index when label(lb)
    # in (input, output)
    ix = Keyword.get(row, :ix)
    # el indicates extra_label(tx-hash or head-hash) when label(lb)
    # in (input, output).
    el = Keyword.get(row, :el)
    # we are sending the bundle query.
    {ok?, {_, qf}, query_state} =
      Helper.bundle_query(bh, el, ts, ix)
    if ok? == :ok do
      # we put the query_state in queries_states map.
      # we return updated acc
      put_elem(acc, 1, Map.put(queries_states, qf, query_state))
    else
      # we break.
      {:error, {:dead_shard_stage, ok?} }
    end
  end

  @doc """
   This function handle the edge row.
  """
  @spec bundle_queries(binary, Keyword.t, tuple) :: tuple
  def bundle_queries(address, [{:lb, 60}|_] = row, {:ok, _, hints} = acc) do
    # lb is hint = 60.
    # el indicates extra_label(recent_timestamp) when
    # label(lb) = hint = 60.
    el = Keyword.get(row, :el)
    # first we convert el(recent_timestamp) to year/month
    # create hint map
    %{year: year, month: month} = DateTime.from_unix(el)
    hint = %{address: address, year: year, month: month}
    put_elem(acc, 2, [hint| hints])
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
