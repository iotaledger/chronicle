defmodule ExtendedApi.Worker.FindTransactions.Approvees.EdgeFn do

  @moduledoc """
    This module hold the function that are going to compute
    the edge query result, it creates tag hint for the tag row.

    finally it returns
    hint as list, mean the tag hint is ready
  """
  alias ExtendedApi.Worker.FindTransactions.Approvees.Helper

  @doc """
   This function handle the edge row for an approve.

   # if lx = 0, then ex is the hash.
   # else if ix is branch(1) and lx > 0, then a bundle query is required to fetch only lx hash
   # else ix is trunk(0) and lx > 0, then a bundle query is required to fetch all hashes trunk < lx, + ex(head_hash).

   # NOTE: the result of this function is passed as acc
   to the next row.
  """
  @spec bundle_queries(Keyword.t, list) :: map
  def bundle_queries([{:lx, 0},{:ix, 0},{:ex, ex} | _], %{hashes: hashes} = acc) do
    %{acc | hashes: [ex | hashes]}
  end

  # lx(last_index/bundle_length) must be greater than zero.
  @spec bundle_queries(Keyword.t, list) :: map
  def bundle_queries([lx: _,ix: ix,ex: ex,v2: v2, ts: ts] = row, %{queries_states: queries_states} = acc) do
    # bh = v2, # id = ex
    {ok?, {_,qf}, query_state} = Helper.bundle_query(ix,v2,ex,ts)
    if ok? == :ok do
      # we put the query_state in queries_states,
      # we return updated acc's hashes/queries_states.
      %{acc | queries_states: [{qf, query_state} | queries_states]}
    else
      # we break.
      {:error, {:dead_shard_stage, ok?}}
    end
  end

  @spec bundle_queries(Keyword.t, {:error, tuple}) :: tuple
  def bundle_queries(row, acc) do
    # we keep breaking
    acc
  end

end
