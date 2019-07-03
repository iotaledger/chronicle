defmodule ExtendedApi.Worker.FindTransactions.Hints.Helper do

  @moduledoc """
    This module hold all the required helper functions
    Which is gonna be used by FindTransactions.Hints worker

  """
  alias ExtendedApi.Worker.FindTransactions.{Hints, Hints.ZeroValueFn, Hints.BundleFn, Hints.TagFn}
  alias Core.DataModel.{Keyspace.Tangle, Table.Bundle, Table.ZeroValue, Table.Tag}
  import OverDB.Builder.Query

  @address_full_hint %{address: nil, month: 1, year: nil, paging_state: nil, page_size: nil}
  @address_rest_hint %{address: nil, month: 12, year: nil, page_size: nil}
  @tag_full_hint %{tag: nil, month: 1, year: nil, paging_state: nil, page_size: nil}
  @tag_rest_hint %{tag: nil, month: 12, year: nil, page_size: nil}

  @bundle_cql "SELECT b FROM tangle.bundle WHERE bh = ? AND lb = ? AND ts = ? AND ix = ?"
  @zero_value_cql "SELECT ts,v2,ix,el FROM tangle.zero_value WHERE v1 = ? AND yy = ? AND mm = ? AND lb = 10"
  @tag_27_cql "SELECT th FROM tangle.tag WHERE p0 = ? AND p1 = ? AND yy = ? AND mm = ? AND p2 = ? AND p3 = ? AND rt = ?"
  @tag_8_cql "SELECT th FROM tangle.tag WHERE p0 = ? AND p1 = ? AND yy = ? AND mm = ? AND p2 = ? AND p3 = ?"
  @tag_6_cql "SELECT th FROM tangle.tag WHERE p0 = ? AND p1 = ? AND yy = ? AND mm = ? AND p2 = ?"
  @tag_4_cql "SELECT th FROM tangle.tag WHERE p0 = ? AND p1 = ? AND yy = ? AND mm = ?"
  @max_bigint 9223372036854775807 # will be used to generate random qf.
  @page_size 500 # this the upper limit of page_size per address.


  # these types check guards
  defguardp is_address(address) when is_binary(address)
  defguardp is_tag(tag) when is_binary(tag) and byte_size(tag) <= 27 and byte_size(tag) >= 4
  defguardp is_month(month) when is_integer(month) and month > 0 and month <= 12
  defguardp is_year(year) when is_integer(year)
  defguardp is_page_size(p_size) when is_integer(p_size) and p_size <= @page_size
  defguardp is_paging_state(p_state) when is_list(p_state)

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
  @spec queries(list,map, list, integer) :: {:ok, map} | {:error, term}
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

  # tag hint _queries functions start
  @spec _queries(list,map, list, integer) :: tuple
  defp _queries(
    [%{"tag" => tag, "paging_state" => p_state, "page_size" => p_size, "year" => yy, "month" => mm} = hint | rest],
    state, queries_states_list, ref)
    when is_tag(tag) and is_year(yy) and is_month(mm) and is_page_size(p_size) and is_paging_state(p_state) do
    {ok?, _, q_s} = tag_query(hint, ref)
    _queries(ok?,rest,state, queries_states_list, ref, q_s)
  end

  @spec _queries(list,map, list, integer) :: tuple
  defp _queries(
    [%{"tag" => tag, "year" => yy, "month" => mm, "page_size" => p_size} = hint| rest],
    state, queries_states_list, ref)
    when is_tag(tag) and is_year(yy) and is_month(mm) and is_page_size(p_size) do
    {ok?, _, q_s} = tag_query(hint, ref)
    _queries(ok?,rest,state, queries_states_list, ref, q_s)
  end
  # tag hint _queries functions end


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
        page_size: p_size, paging_state: hint["paging_state"]})
    |> pk([v1: address, yy: yy, mm: mm]) |> prepare?(true) |> reference({:zero_value, ref})
    |> Hints.query()
  end

  @spec tag_query(map, integer, nil | map) :: tuple
  def tag_query(
    %{"tag" => tag,"year" => yy, "month" => mm, "page_size" => p_size} = hint,
     ref, opts \\ nil) do
    query =
      {Tangle, Tag}
      |> select([:th]) |> type(:stream)
      |> assign(hint: hint)
      |> opts(opts || %{function: {TagFn, :construct},
          page_size: p_size, paging_state: hint["paging_state"]})
      |> pk([p0: p0, p1: p1, yy: yy, mm: mm]) |> prepare?(true) |> reference({:tag, ref})
    # we pattern matching the tag into pairs.
    case tag do
      # this create full tag query.
      <<p0::2-bytes, p1::2-bytes, p2::2-bytes,p3::2-bytes,rt::19-bytes>> ->
        query
        |> cql(@tag_27_cql) #
        |> values([{:blob, p0},{:blob, p1},{:smallint, yy},{:smallint, mm},{:blob, p2},{:blob, p3},{:blob, rt}])
        |> Hints.query()
      # this create 8-chars query in IAC is 275m area.
      <<p0::2-bytes, p1::2-bytes, p2::2-bytes,p3::2-bytes,_::binary>> ->
        query
        |> cql(@tag_8_cql) #
        |> values([{:blob, p0},{:blob, p1},{:smallint, yy},{:smallint, mm},{:blob, p2},{:blob, p3}])
        |> Hints.query()
      # this create 6-chars query in IAC is 5.5km area.
      <<p0::2-bytes, p1::2-bytes, p2::2-bytes,_::binary>> ->
        query
        |> cql(@tag_6_cql) #
        |> values([{:blob, p0},{:blob, p1},{:smallint, yy},{:smallint, mm},{:blob, p2}])
        |> Hints.query()
      # this create 4-chars query in IAC is 110km area.
      <<p0::2-bytes, p1::2-bytes,_::binary>> ->
        query
        |> cql(@tag_4_cql) #
        |> values([{:blob, p0},{:blob, p1},{:smallint, yy},{:smallint, mm}])
        |> Hints.query()
    end
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

  # creating address hint functions
  @spec address_hint(map, nil | binary) :: map
  def address_hint(hint, paging_state \\ nil)
  def address_hint(%{"month" => 1, "year" => yy, "address" => address, "page_size" => p_size} = _hint, paging_state) do
    if paging_state do
      %{@address_full_hint | address: address, year: yy, paging_state: :binary.bin_to_list(paging_state), page_size: p_size }
    else
      %{@address_rest_hint | address: address, year: yy-1, page_size: p_size}
    end
  end
  @spec address_hint(map, nil | binary) :: map
  def address_hint(%{"month" => mm, "year" => yy, "address" => address, "page_size" => p_size}, paging_state) do
    if paging_state do
      %{@address_full_hint | address: address, month: mm, year: yy, paging_state: :binary.bin_to_list(paging_state), page_size: p_size }
    else
      %{@address_rest_hint | address: address, month: mm-1, year: yy, page_size: p_size}
    end
  end

  # creating tag hint functions
  @spec address_hint(map, nil | binary) :: map
  def tag_hint(hint, paging_state \\ nil)
  def tag_hint(%{"month" => 1, "year" => yy, "tag" => tag, "page_size" => p_size} = _hint, paging_state) do
    if paging_state do
      %{@tag_full_hint | tag: tag, year: yy, paging_state: :binary.bin_to_list(paging_state), page_size: p_size }
    else
      %{@tag_rest_hint | tag: tag, year: yy-1, page_size: p_size}
    end
  end
  @spec tag_hint(map, nil | binary) :: map
  def tag_hint(%{"month" => mm, "year" => yy, "tag" => tag, "page_size" => p_size}, paging_state) do
    if paging_state do
      %{@tag_full_hint | tag: tag, month: mm, year: yy, paging_state: :binary.bin_to_list(paging_state), page_size: p_size }
    else
      %{@tag_rest_hint | tag: tag, month: mm-1, year: yy, page_size: p_size}
    end
  end

end
