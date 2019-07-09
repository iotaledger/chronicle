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
  @spec queries(list, map, list, integer) :: {:ok, map} | {:error, term}
  def queries(bundle_hashes, state, queries_states_list \\ [], ref \\ 0)
  def queries(bundle_hashes, state, queries_states_list, ref) do
    _queries(bundle_hashes, state, queries_states_list, ref)
  end

  @spec _queries(list, map,list, integer) :: tuple
  defp _queries([bundle_hash | rest], state, queries_states_list, ref) when is_binary(bundle_hash) do
    {ok?, _, q_s} = bundle_query(bundle_hash, ref)
    _queries(ok?, rest, state, queries_states_list, ref, q_s)
  end

  @spec _queries(list, map, list, integer) :: tuple
  defp _queries([], state, queries_states_list, ref) do
    # ref indicates the total number of queries.
    # [] is the initial hashes_list
    # state is the state which hold all the queries_states.
    {:ok, Enum.into(queries_states_list, %{ref: ref, hashes: []}) |> Map.merge(state)}
  end

  @spec _queries(list, integer,list, map) :: tuple
  defp _queries(_, _, _,_) do
    {:error, :invalid}
  end

  @spec _queries(atom, list, map,list, integer, map) :: tuple
  defp _queries(:ok ,rest, state,queries_states_list, ref, q_s) do
    # :ok indicates ref => q_s has been received by the shard's stage.
    # therefore we should put that in state and increase the ref.
    # now loop through the rest with updated ref/queries_states_list.
    _queries(rest, state, [{ref, q_s} | queries_states_list], ref+1)
  end

  @spec _queries(term, list, map,list, integer, map) :: tuple
  defp _queries(ok?,_, _, _, _,_) do
    {:error, ok?}
  end

  @doc """
    This function generates the query for a given bundle hash.
    it takes bundle-hash and ref which will be used to trace the response.
  """
  @spec bundle_query(binary, integer, map) :: tuple
  def bundle_query(bundle_hash, ref, opts \\ %{function: {BundleFn, :construct}}) do
    {Tangle, Bundle}
    |> select([:b]) |> type(:stream) |> assign(bundle_hash: bundle_hash)
    |> cql("SELECT b FROM tangle.bundle WHERE bh = ? AND lb = 30")
    |> values([{:blob, bundle_hash}])
    |> opts(opts)
    |> pk([bh: bundle_hash]) |> prepare?(true) |> reference({:bundle, ref})
    |> Bundles.query()
  end


end
