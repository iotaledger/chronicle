defmodule ExtendedApi.Worker.FindTransactions.Addresses.Helper do

  @moduledoc """
    This module hold all the required helper functions
    Which is gonna be used by FindTransactions.Bundles worker and its
    row compute module bundle_fn.ex.
  """
  alias ExtendedApi.Worker.FindTransactions.{Addresses, Addresses.EdgeFn, Addresses.BundleFn}
  alias Core.DataModel.{Keyspace.Tangle, Table.Bundle, Table.Edge}
  import OverDB.Builder.Query

  @max_bigint 9223372036854775807 # will be used to generate random qf.
  @initial_acc %{queries_states: [], hint: []} # initial acc
  @bundle_cql "SELECT b FROM tangle.bundle WHERE bh = ? AND lb = ? AND ts = ? AND ix = ?"
  @edge_cql "SELECT lb,el,ts,v2,ix FROM tangle.edge WHERE v1 = ? AND lb IN ?"
  # Start of Helper functions for edge table queries ###########################

  @doc """
    This function takes Addresses as list and Worker State then
    return tuple
      {:ok, ref, state} :  ref is the total number of queries. and state
        is updated map which include all the new queries_states.
      {:error, term} : error occurs either because of invalid
        bundle-hash type or dead shard stage in the query engine.
  """
  @spec queries(list, map,list integer) :: {:ok, map} | {:error, term}
  def queries(addresses, state, queries_states_list \\ [], ref \\ 0)
  def queries(addresses, state, queries_states_list, ref) do
    _queries(addresses, state, queries_states_list, ref)
  end

  @spec _queries(list, map,list, integer) :: tuple
  defp _queries([address | rest], state,queries_states_list, ref) when is_binary(address) do
    {ok?, _, q_s} = edge_query(address, ref)
    _queries(ok?, rest, state,queries_states_list, ref, q_s)
  end

  @spec _queries(list, map,list, integer) :: tuple
  defp _queries([], state, queries_states_list, ref) do
    # ref indicates the total number of queries.
    # [] is the initial hashes_list/hints_list.
    # state is the state which hold all the queries_states.
    # updated worker's state.
    {:ok, Enum.into(queries_states_list, %{ref: ref, hashes: [], hints: [] }) |> Map.merge(state)}
  end

  @spec _queries(list, map,list, integer) :: tuple
  defp _queries(_, _, _,_) do
    {:error, :invalid_type}
  end

  @spec _queries(atom, list, map,list, integer, map) :: tuple
  defp _queries(:ok ,rest, state,queries_states_list, ref, q_s) do
    # :ok indicates ref => q_s has been received by the shard's stage.
    # therefore we should put that in state and increase the ref.
    # now loop through the rest with updated ref/state.
    _queries(rest, state,[{ref,q_s} | queries_states_list], ref+1)
  end

  @spec _queries(term, list, map,list, integer, map) :: tuple
  defp _queries(ok?,_, _, _, _,_) do
    {:error, ok?}
  end

  @spec edge_query(binary, integer, nil | map) :: tuple
  def edge_query(address, ref, opts \\ nil) do
    {Tangle, Edge}
    |> select([:lb,:el,:ts,:v2,:ix]) |> type(:stream)
    |> assign(address: address, acc: @initial_acc)
    |> cql(@edge_cql)
    |> values([{:varchar, address}, {{:list, :tinyint}, [10,20,60]}])
    |> opts(opts || %{function: {EdgeFn, :bundle_queries, [address]}})
    |> pk([v1: address]) |> prepare?(true) |> reference({:edge, ref})
    |> Addresses.query()
  end

  # Start of Helper functions for bundle table queries ###########################

  @doc """
    This function generates the query for a given bundle hash.
    it takes bundle_hash, label, timestamp, current_index.
    the expected query result should be a list of rows where
    each row should contain transaction hash at current_index.
    for example: current_index = 0 for a bundle_hash_x.
    should return all the transactions_hashes(re/attachment)
    at index = 0.
    Anyway this function returns tuple.
  """
  @spec bundle_query(binary, integer, integer, integer, map) :: tuple
  def bundle_query(bundle_hash, label, ts, ix, opts \\ %{function: {BundleFn, :construct}}) do
    {Tangle, Bundle}
    |> select([:b]) |> type(:stream)
    |> assign(bundle_hash: bundle_hash, current_index: ix, label: label, timestamp: ts)
    |> cql(@bundle_cql)
    |> values([{:varchar, bundle_hash}, {:tinyint, label}, {:varint, ts}, {:varint, ix}])
    |> opts(opts)
    |> pk([bh: bundle_hash]) |> prepare?(true) |> reference({:bundle, :rand.uniform(@max_bigint)})
    |> Addresses.query()
  end
end
