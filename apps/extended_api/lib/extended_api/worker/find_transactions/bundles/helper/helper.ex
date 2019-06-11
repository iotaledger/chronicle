defmodule ExtendedApi.Worker.FindTransactions.Bundles.Helper do

  @moduledoc """
    This module hold all the required helper functions
    Which is gonna be used by FindTransactions.Bundles worker and its
    row compute module bundle_fn.ex.
  """
  alias ExtendedApi.Worker.FindTransactions.{Bundles, Bundles.BundleFn}
  alias Core.DataModel.{Keyspace.Tangle, Table.Bundle}
  import OverDB.Builder.Query

  # Start of Helper functions for Bundle table queries ###########################

  @doc """
    This function takes bundles_hashes as list and Worker State then
    return tuple
      {:ok, ref, state} :  ref is the total number of queries. and state
        is updated map which include all the new queries_states.
      {:error, term} : error occurs either because of invalid
        bundle-hash type or dead shard stage in the query engine.
  """
  @spec queries(list, map, integer) :: {:ok, integer, map} | {:error, term}
  def queries(bundle_hashes, state, ref \\ 0)
  def queries(bundle_hashes, state, ref) do
    bundle_hashes_bundle_queries(bundle_hashes, state, ref)
  end

  @spec bundle_hashes_bundle_queries(list, map, integer) :: tuple
  defp bundle_hashes_bundle_queries([bundle_hash | rest], state, ref) when is_binary(bundle_hash) do
    {ok?, _, q_s} = bundle_hash_bundle_query(bundle_hash, ref)
    _bundle_hashes_bundle_queries(ok?, rest, state, ref, q_s)
  end

  @spec bundle_hashes_bundle_queries(list, map, integer) :: tuple
  defp bundle_hashes_bundle_queries([], state, ref) do
    # ref indicates the total number of queries.
    # [] is the initial hashes_list
    # state is the state_map which hold all the queries_states.
    {:ok, {{ref, []}, state}}
  end

  @spec bundle_hashes_bundle_queries(list, integer, map) :: tuple
  defp bundle_hashes_bundle_queries(_, _, _) do
    {:error, :invalid_type}
  end

  @spec _bundle_hashes_bundle_queries(atom, list, map, integer, map) :: tuple
  defp _bundle_hashes_bundle_queries(:ok ,rest, state, ref, q_s) do
    # :ok indicates ref => q_s has been received by the shard's stage.
    # therefore we should put that in state and increase the ref.
    {new_ref, state} = _put_query_state(ref, q_s, state)
    # now loop through the rest with updated ref/state.
    bundle_hashes_bundle_queries(rest, state, new_ref)
  end

  @spec _bundle_hashes_bundle_queries(term, list, map, integer, map) :: tuple
  defp _bundle_hashes_bundle_queries(ok?,_, _, _, _) do
    {:error, ok?}
  end

  @spec _put_query_state(integer, map, map) :: tuple
  defp _put_query_state(ref, q_s, state) do
    state = Map.put(state, ref, q_s)
    {ref+1, state}
  end

  @doc """
    This function generates the query for a given bundle hash.
    it takes bundle-hash and ref which will be used to trace the response.
  """
  @spec bundle_hash_bundle_query(binary, integer, map) :: tuple
  def bundle_hash_bundle_query(bundle_hash, ref, opts \\ %{function: {BundleFn, :construct}}) do
    {Tangle, Bundle}
    |> select([:b]) |> type(:stream) |> assign(bundle_hash: bundle_hash)
    |> cql("SELECT b FROM tangle.bundle WHERE bh = ? AND lb IN ?")
    |> values([{:varchar, bundle_hash}, {{:list, :tinyint}, [30,40]}])
    |> opts(opts)
    |> pk([bh: bundle_hash]) |> prepare?(true) |> reference({:bundle, ref})
    |> Bundles.query()
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
