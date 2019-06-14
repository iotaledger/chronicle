defmodule ExtendedApi.Worker.GetTrytes.EdgeFn do


  @moduledoc """
    This module hold the function that are going to compute
    the edge query result, it creates one query
    to fetch two rows
    1- address's information row (fixed_fields).
    2- transaction's information row (dymanic_fields).

    finally it returns
    {:ok, query_state} mean the query has been received
    by shard's stage (reporter)
    or {:error, reason} if the reporter/shardstage is dead.
  """
  alias ExtendedApi.Worker.GetTrytes.Helper

  @doc """
   This function handle the edge row.
  """
  @spec bundle_queries(integer, Keyword.t, list) :: tuple
  def bundle_queries(
    ref,
    [lb: lb, ts: ts, v2: bh, ex: ex, ix: ix, el: el, lx: lx],
    _) do
    # lb indicates label (tx_hash, or h_hash)
    # ts indicates bundle_timestamp
    # v2(bh) indicates bundle_hash/bh
    # ex indicates h_hash/id
    # ix indicates current_index
    # el indicates whether input or output.
    # lx indicates last_index.
    # we are sending the bundle query.
    {ok?, _, query_state} =
      Helper.bundle_query(bh, el,lb, ts, ix,lx,ex, ref)
    if ok? == :ok do
      # we return query state
      {:ok, query_state}
    else
      # we break,
      {:error, {:dead_shard_stage, ok?} }
    end
  end



end
