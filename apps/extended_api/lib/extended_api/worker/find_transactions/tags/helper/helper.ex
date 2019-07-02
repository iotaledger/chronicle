defmodule ExtendedApi.Worker.FindTransactions.Tags.Helper do

  @moduledoc """
    This module hold all the required helper functions
    Which is gonna be used by FindTransactions.Tags worker
  """

  alias ExtendedApi.Worker.FindTransactions.{Tags, Tags.EdgeFn}
  alias Core.DataModel.{Keyspace.Tangle, Table.Edge}
  import OverDB.Builder.Query

  # these types check guards
  defguard is_tag(tag) when is_binary(tag) and byte_size(tag) == 27

  @edge_cql "SELECT el FROM tangle.edge WHERE v1 = ? AND lb = 70"

  # Start of Helper functions for edge table queries ###########################

  @doc """
    This function takes Tags as list and Worker State then
    return tuple
      {:ok, state} :
        state is updated map which include all the new queries_states,
        and initial hashes/hints.
      {:error, term} : error occurs either because of invalid
        tag structure/type or dead shard stage in the query engine.
  """
  @spec queries(list,map, list, integer) :: {:ok, map} | {:error, term}
  def queries(tags, state, queries_states_list \\ [], ref \\ 0)
  def queries(tags, state, queries_states_list, ref) do
    _queries(tags, state, queries_states_list, ref)
  end

  @spec _queries(list,map, list, integer) :: tuple
  defp _queries([tag | rest], state, queries_states_list, ref) when is_tag(tag) do
    {ok?, _, q_s} = edge_query(tag, ref)
    _queries(ok?,rest,state, queries_states_list, ref, q_s)
  end

  @spec _queries(list,map, list, integer) :: tuple
  defp _queries([],%{from: from}, queries_states_list, ref) do
    # ref indicates the total number of queries.
    # queries_states_list is a list which hold all the
    # generated queries_states that needed to decode future responses.
    state =
      Enum.into(
        queries_states_list,
        %{ref: ref, hints: [], from: from}
        )
    # return state to worker
    {:ok, state}
  end

  @spec _queries(list,map, list, integer) :: tuple
  defp _queries(_,_, _, _) do
    {:error, :invalid}
  end

  @spec _queries(atom, list, map, list, integer, map) :: tuple
  defp _queries(:ok,rest,state, queries_states_list, ref, q_s) do
    # :ok indicates ref => q_s has been received by the shard's stage.
    # therefore we should put that in queries_states_list and increase the ref.
    # now loop through the rest with updated ref/queries_states_list.
    _queries(rest,state, [{ref, q_s} | queries_states_list], ref+1)
  end

  @spec _queries(term, list, map, list, integer, map) :: tuple
  defp _queries(ok?,_,_, _, _, _) do
    {:error, ok?}
  end

  @spec edge_query(map, integer, nil | map) :: tuple
  def edge_query(tag,ref, opts \\ nil) do
    {Tangle, Edge}
    |> select([:el]) |> type(:stream)
    |> assign(tag: tag)
    |> cql(@edge_cql) 
    |> values([{:blob, tag}])
    |> opts(opts || %{function: {EdgeFn, :create_hint, [tag]}})
    |> pk([v1: tag]) |> prepare?(true) |> reference({:edge, ref})
    |> Tags.query()
  end

end
