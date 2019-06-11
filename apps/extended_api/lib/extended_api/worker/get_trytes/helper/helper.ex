defmodule ExtendedApi.Worker.GetTrytes.Helper do

  @moduledoc """
    This module hold all the required helper functions
    Which is gonna be used by GetTrytes worker and its
    row compute modules(bundle_fn.ex, edge_fn.ex)
  """
  alias ExtendedApi.Worker.{GetTrytes, GetTrytes.BundleFn, GetTrytes.EdgeFn}
  alias Core.DataModel.{Keyspace.Tangle, Table.Bundle, Table.Edge}
  import OverDB.Builder.Query

  # Start of Helper functions for Edge table queries ###########################

  @spec edge_queries(list, map, list, integer) :: tuple
  def edge_queries(hashes, state, trytes_list \\ [], ref \\ 0)
  def edge_queries([hash | rest_hashes], state, trytes_list, ref) when is_binary(hash) do
    # first we create edge query
    {ok?, _,q_s} = edge_query(hash, ref)
    # we ask this function to put query_state and proceed or break.
    _edge_queries(ok?, rest_hashes, state, trytes_list, ref, q_s)
  end

  @spec edge_queries(list, map, list, integer) :: tuple
  def edge_queries([], state, trytes_list, ref) do
    {:ok, { {ref, trytes_list}, state} }
  end

  @spec edge_queries(list, map, list, integer) :: tuple
  def edge_queries(_, _, _, _) do
    {:error, :invalid_type}
  end

  @spec _edge_queries(atom, list, map, list, integer, map) :: tuple
  defp _edge_queries(:ok ,rest_hashes, state, trytes_list, ref, q_s) do
    # :ok indicates ref => q_s has been received by the shard's stage.
    # therefore we should put that in state and increase the ref.
    {new_ref, trytes_list, state} = _put_query_state(trytes_list, ref, q_s, state)
    # now loop through the rest_hashes with updated ref/trytes_list.
    edge_queries(rest_hashes, state, trytes_list, new_ref)
  end

  @spec _edge_queries(term, list, map, list, integer, map) :: tuple
  defp _edge_queries(ok?,_, _, _, _, _) do
    {:error, ok?}
  end

  @spec edge_query(binary, integer) :: tuple
  def edge_query(hash, ref) do
    {Tangle, Edge}
    |> select([]) |> type(:stream) |> assign(hash: hash)
    |> cql("SELECT * FROM tangle.edge WHERE v1 = ? AND lb IN ?")
    |> values([{:varchar, hash}, {{:list, :tinyint}, [30,40]}])
    |> opts(%{function: {EdgeFn, :bundle_queries, [ref]}})
    |> pk([v1: hash]) |> prepare?(true) |> reference({:edge, ref})
    |> GetTrytes.query()
  end

  @spec _put_query_state(list, integer, map, map) :: tuple
  defp _put_query_state(trytes_list, ref, q_s, state) do
    state = Map.put(state, ref, q_s)
    {ref+1, [nil | trytes_list], state}
  end

  # Start of Helper functions for Bundle table queries #########################

  @doc """
    This function generates and execute bundle query.
  """
  def bundle_query(bh,addr_lb,tx_lb,ts,ix,lx,ex,ref, opts \\ nil) do
    {Tangle, Bundle}
    |> select([:lb, :va, :a, :c, :d, :e, :f, :g, :h, :i]) |> type(:stream)
     # NOTE: we had to use this statement till ScyllaDB's bug get resolved (https://github.com/scylladb/scylla/issues/4509)
    |> cql("SELECT lb,va,a,c,d,e,f,g,h,i FROM tangle.bundle WHERE bh = ? AND lb IN ? AND ts = ? AND ix = ? AND id IN ?")
    |> values([{:varchar, bh}, {{:list, :tinyint}, [addr_lb, tx_lb]}, {:varint, ts}, {:varint, ix}, {{:list, :varchar}, ["addr", ex]}])
    |> pk([bh: bh]) |> prepare?(true) |> reference({:bundle, ref})
    |> opts(opts || %{function: {BundleFn, :construct, [bh,addr_lb,tx_lb,ts,ix,lx,ex,ref]}})
    |> GetTrytes.query()
  end

  @doc """
    This function generates and execute bundle_query from opts
    It's intended to make sure to add the paging_state(if any)
  """
  @spec bundle_query_from_opts(map) :: tuple
  def bundle_query_from_opts(%{function: {_, _, args}} = opts) do
    apply(Helper, :bundle_query, args ++ [opts])
  end

end
