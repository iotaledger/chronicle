defmodule ExtendedApi.Worker.FindTransactions.Hints.Helper do

  # NOTE: WIP
  @moduledoc """
    This module hold all the required helper functions
    Which is gonna be used by FindTransactions.Hints worker

  """
  alias ExtendedApi.Worker.FindTransactions.{Hints, Hints.ZeroValueFn, Hints.BundleFn}
  alias Core.DataModel.{Keyspace.Tangle, Table.Bundle, Table.ZeroValue}
  import OverDB.Builder.Query

  @full_hint %{address: nil, month: 1, year: nil, paging_state: nil, page_size: nil}
  @rest_hint %{address: nil, month: 12, year: nil, page_size: nil}

  @zero_value_cql "SELECT ts,v2,ix,el FROM tangle.zero_value WHERE v1 = ? AND yy = ? AND mm = ? AND lb = 10"
  @bundle_cql "SELECT b FROM tangle.bundle WHERE bh = ? AND lb = ? AND ts = ? AND ix = ?"
  @max_bigint 9223372036854775807 # will be used to generate random qf.
  @page_size 500 # this the upper limit of page_size per address.


  # these types check guards
  defguard is_address(address) when is_binary(address)
  defguard is_month(month) when is_integer(month) and month > 0 and month <= 12
  defguard is_year(year) when is_integer(year)
  defguard is_page_size(p_size) when is_integer(p_size) and p_size <= @page_size
  defguard is_paging_state(p_state) when is_binary(p_state)

  # Start of Helper functions for zero_value table queries ###########################

  @doc """
    This function takes Hints as list and Worker State then
    return tuple
      {:ok, state} :
        state is updated map which include all the new queries_states,
        and initial hashes/hints.
      {:error, term} : error occurs either because of invalid
        hint structure/type or dead shard stage in the query engine.
  """
  @spec queries(list,map, list, integer) :: {:ok, integer, map} | {:error, term}
  def queries(hints, state, queries_states_list \\ [], ref \\ 0)
  def queries(hints, state, queries_states_list, ref) do
    _queries(hints, state, queries_states_list, ref)
  end

  @spec _queries(list,map, list, integer) :: tuple
  defp _queries(
    [%{"paging_state" => p_state, "page_size" => p_size, "address" => ad, "year" => yy, "month" => mm} = hint | rest],
    state, queries_states_list, ref)
    when is_address(ad) and is_year(yy) and is_month(mm) and is_page_size(p_size) and is_paging_state(p_state) do
    {ok?, _, q_s} = zero_value_query(hint, ref)
    _queries(ok?,rest,state, queries_states_list, ref, q_s)
  end

  @spec _queries(list,map, list, integer) :: tuple
  defp _queries(
    [%{"address" => ad, "year" => yy, "month" => mm, "page_size" => p_size} = hint| rest],
    state, queries_states_list, ref)
    when is_address(ad) and is_year(yy) and is_month(mm) and is_page_size(p_size) do
    {ok?, _, q_s} = zero_value_query(hint, ref)
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
        %{ref: ref, hashes: [], hints: [], from: from}
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


  @spec zero_value_query(map, integer, nil | map) :: tuple
  def zero_value_query(
    %{"address" => address, "year" => yy, "month" => mm, "page_size" => p_size} = hint,
     ref, opts \\ nil) do
    {Tangle, ZeroValue}
    |> select([:ts,:v2,:ix,:el]) |> type(:stream)
    |> assign(hint: hint)
    |> cql(@zero_value_cql)
    |> values([{:varchar, address},{:smallint, yy},{:smallint, mm}])
    |> opts(opts || %{function: {ZeroValueFn, :bundle_queries},
        page_size: p_size, paging_state: hint[:paging_state]})
    |> pk([v1: address, yy: yy, mm: mm]) |> prepare?(true) |> reference({:zero_value, ref})
    |> Hints.query()
  end

  # Start of Helper functions for bundle table queries ###########################

  @doc """
    This function generates the query for a given bundle hash.
    it takes bundle_hash, label, timestamp, current_index.
    the expected query result should be a list of rows where
    each row should contain transaction hashs at current_index.
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
    |> Hints.query()
  end

  # creating hint functions

  @spec create_hint(map, nil | binary) :: map
  def create_hint(hint, paging_state \\ nil)
  def create_hint(%{"month" => 1, "year" => yy, "address" => address, "page_size" => p_size} = hint, paging_state) do
    if paging_state do
      %{@full_hint | address: address, year: yy, paging_state: paging_state, page_size: p_size }
    else
      %{@rest_hint | address: address, year: yy-1, page_size: p_size}
    end
  end
  @spec create_hint(map, nil | binary) :: map
  def create_hint(%{"month" => mm, "year" => yy, "address" => address, "page_size" => p_size}, paging_state) do
    if paging_state do
      %{@full_hint | address: address, month: mm, year: yy, paging_state: paging_state, page_size: p_size }
    else
      %{@rest_hint | address: address, month: mm-1, year: yy, page_size: p_size}
    end
  end

end
