defmodule ExtendedApi.Worker.GetTrytes.Helper do

  @moduledoc """
    This module hold all the required helper functions
    Which is gonna be used by GetTrytes worker and its
    row compute modules(bundle_fn.ex, edge_fn.ex)
  """
  alias ExtendedApi.Worker.{GetTrytes, GetTrytes.BundleFn, GetTrytes.EdgeFn}
  alias ExtendedApi.Worker.GetTrytes.Helper
  alias Core.DataModel.{Keyspace.Tangle, Table.Bundle, Table.Edge}
  import OverDB.Builder.Query

  @edge_cql "SELECT lb,ts,v2,ex,ix,el,lx FROM tangle.edge WHERE v1 = ? AND lb = 30"
  @bundle_cql "SELECT lb,va,a,c,d,e,f,g,h,i FROM tangle.bundle WHERE bh = ? AND lb IN ? AND ts = ? AND ix = ? AND id IN ?"
  # Start of Helper functions for Edge table queries ###########################

  @spec queries(list, map,list, list, integer) :: tuple
  def queries(hashes, state, queries_states_list \\ [], trytes_list \\ [], ref \\ 0)
  def queries(hashes, state, queries_states_list, trytes_list, ref) do
    _queries(hashes, state, queries_states_list, trytes_list, ref)
  end

  @spec queries(list, map,list, list, integer) :: tuple
  defp _queries([hash | rest], state, queries_states_list, trytes_list, ref) when is_binary(hash) do
    # first we create edge query
    {ok?, _,q_s} = edge_query(hash, ref)
    # we ask this function to put query_state and proceed or break.
    _queries(ok?, rest, state, queries_states_list, trytes_list, ref, q_s)
  end

  @spec _queries(list, map, list,list, integer) :: tuple
  defp _queries([], state, queries_states_list, trytes_list, ref) do
    {:ok, Enum.into(queries_states_list, %{ref: ref, trytes: trytes_list})|> Map.merge(state)}
  end

  @spec _queries(list, map, list,list, integer) :: tuple
  defp _queries(_, _, _, _,_) do
    {:error, :invalid}
  end

  @spec _queries(atom, list, map, list,list, integer, map) :: tuple
  defp _queries(:ok ,rest, state, queries_states_list, trytes_list, ref, q_s) do
    # :ok indicates ref => q_s has been received by the shard's stage.
    # now loop through the rest(rest_hashes) with updated queries_states_list/ref/trytes_list.
    _queries(rest, state, [{ref, q_s} | queries_states_list], [nil | trytes_list], ref+1)
  end

  @spec _queries(term, list, map, list,list, integer, map) :: tuple
  defp _queries(ok?,_, _, _, _, _,_) do
    {:error, ok?}
  end

  @spec edge_query(binary, integer, map) :: tuple
  def edge_query(hash, ref, opts \\ nil) do
    {Tangle, Edge}
    |> select([:lb,:ts,:v2,:ex,:ix,:el,:lx]) |> type(:stream) |> assign(hash: hash)
    |> cql(@edge_cql)
    |> values([{:blob, hash}])
    |> opts(opts || %{function: {EdgeFn, :bundle_queries, [ref]}})
    |> pk([v1: hash]) |> prepare?(true) |> reference({:edge, ref})
    |> GetTrytes.query()
  end

  # Start of Helper functions for Bundle table queries #########################

  @doc """
    This function generates and execute bundle query.
  """
  def bundle_query(bh,addr_lb,tx_lb,ts,ix,lx,ex,ref, opts \\ nil, acc \\ %{}) do
    {Tangle, Bundle}
    |> select([:lb, :va, :a, :c, :d, :e, :f, :g, :h, :i]) |> type(:stream)
     # NOTE: we had to use this statement till ScyllaDB's bug get resolved (https://github.com/scylladb/scylla/issues/4509)
    |> cql(@bundle_cql) # check at the top of module to know the current cql statement.
    |> assign(acc: acc)
    |> values([{:blob, bh}, {{:list, :tinyint}, [addr_lb, tx_lb]}, {:varint, ts}, {:varint, ix}, {{:list, :blob}, ["addr", ex]}])
    |> pk([bh: bh]) |> prepare?(true) |> reference({:bundle, ref})
    |> opts(opts || %{function: {BundleFn, :construct, [bh,addr_lb,tx_lb,ts,ix,lx,ex,ref]}})
    |> GetTrytes.query()
  end

  @doc """
    This function generates and execute bundle_query from opts
    It's intended to make sure to add the paging_state(if any)
    and append the arguments ( bh, addr_lb,tx_lb, etc)
  """
  @spec bundle_query_from_opts_acc(map,map) :: tuple
  def bundle_query_from_opts_acc(%{function: {_, _, args}} = opts, acc) do
    apply(ExtendedApi.Worker.GetTrytes.Helper, :bundle_query, args ++ [opts,acc])
  end

end
