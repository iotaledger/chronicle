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
  def bundle_queries(
      _address,
      [lb: lb, el: el, ts: ts, v2: bh, ix: ix],
      %{queries_states: queries_states} = acc)
      when lb != 60 do # lb != 60 mean it's not a hint.
    # lb indicates label (output, input)....^
    # ts indicates bundle_timestamp when label(lb)
    # in (input, output)
    # v2(bh) indicates bundle_hash.
    # ix indicates current_index when label(lb)
    # in (input, output)
    # el indicates extra_label(tx-hash or head-hash) when label(lb)
    # in (input, output).
    # we are sending the bundle query.
    {ok?, {_, qf}, query_state} =
      Helper.bundle_query(bh, el, ts, ix)
    if ok? == :ok do
      # we put the query_state in queries_states map.
      # we return updated acc
      %{acc | queries_states: [{qf, query_state} | queries_states]}
    else
      # we break.
      {:error, {:dead_shard_stage, ok?} }
    end
  end

  @doc """
   This function handle the edge row.
  """
  @spec bundle_queries(binary, Keyword.t, map) :: tuple
  def bundle_queries(address, [{:lb, 60},{:el, el}|_], acc) when is_map(acc) do
    # lb is hint = 60.
    # el indicates extra_label(recent_timestamp) when
    # label(lb) = hint = 60.
    # first we convert el(recent_timestamp) to year/month
    # create hint map
    %{year: year, month: month} = DateTime.from_unix(el)
    # there is possibility for one hint only per address.
    hint = %{address: address, year: year, month: month}
    %{acc | hint: hint}
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
