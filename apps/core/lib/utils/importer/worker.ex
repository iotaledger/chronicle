defmodule Core.Utils.Importer.Worker do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias Core.DataModel.{Keyspace.Tangle, Table.Bundle, Table.Edge, Table.ZeroValue}
  alias Core.Utils.Struct.Transaction
  @bundle_cql_for_address_row "INSERT INTO tangle.bundle (bh,lb,ts,ix,id,va,a,d,e,g) VALUES (?,?,?,?,?,?,?,?,?,?)"
  @bundle_cql_for_tx_row "INSERT INTO tangle.bundle (bh,lb,ts,ix,id,va,a,b,c,d,e,f,g,h,i) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
  @edge_cql_for_address_row "INSERT INTO tangle.edge (v1,lb,ts,v2,ex,ix,el,lx) VALUES (?,?,?,?,varintAsBlob(?),?,?,?)"
  @edge_cql_for_tx_row "INSERT INTO tangle.edge (v1,lb,ts,v2,ex,ix,el,lx,sx) VALUES (?,?,?,?,?,?,?,?,?)"
  @edge_hint_cql_for_address_row "INSERT INTO tangle.edge (v1,lb,ts,v2,ex,ix,el,lx) VALUES (?,60,0,varintAsBlob(0),varintAsBlob(0),0,?,?)"
  @zero_value_cql_for_address_row "INSERT INTO tangle.zero_value (v1,yy,mm,lb,ts,v2,ex,ix,el,lx) VALUES (?,?,?,10,?,?,varintAsBlob(0),?,?,?)"

  @doc """
    This function handing starting Importer.worker.
  """
  @spec start_link(list) :: tuple
  def start_link(bundle) do
    state =
      if Enum.all?(bundle, fn(%{value: value}) -> value == 0 end) do
        # it's zero_value bundle.
        {:zero_value, bundle}
      else
        # it's value bundle.
        {:value, bundle}
      end
    GenServer.start_link(__MODULE__, state)
  end

  @doc """
    This function initate Importer.worker
  """
  @spec init(tuple) :: tuple
  def init({:zero_value, bundle}) do
    # create zero_value bundle queries.
    Process.send_after(self(), :check, 10000)
    state = queries(true, bundle) |> Enum.into(%{})
    {:ok, state}
  end

  @doc """
    This function initate Importer.worker
  """
  @spec init(tuple) :: tuple
  def init({:value, bundle}) do
    # create value bundle queries.
    Process.send_after(self(), :check, 10000)
    state = queries(false, bundle) |> Enum.into(%{})
    {:ok, state}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:full, qf, buffer}, state) do
    # first we fetch the query state from the state using the qf key.
    case Protocol.decode_full(buffer, state[qf]) do
      :void ->
        {:noreply, Map.delete(state, qf)}
      err? ->
        IO.inspect(err?)
        {:noreply, state}
    end
  end

  def handle_info(:check, state) do
    size = map_size(state)
    if size > 0 do
      IO.inspect(size)
      Process.send_after(self(), :check, 10000)
      {:noreply, state}
    else
      {:stop, :normal, state}
    end
  end

  @doc """
    Handler function which validate if query request
    has ended in the shard's socket tcp_window.
  """
  def handle_cast({:send?, _query_ref, :ok}, state) do
    {:noreply, state}
  end

  @doc """
    This match unsuccessful send requests.for simplcity
    we are droping the API call in case off any
    unsuccessful send request.
  """
  def handle_cast({:send?, _, status}, %{from: from} = state) do
    IO.inspect("bundle fail")
    {:stop, :normal, state}
  end


  def queries(zero_value?,[%{hash: head_hash} | _] = bundle) do
    _queries(zero_value?, bundle, head_hash, [])
  end

  defp _queries(zero_value?, [tx | rest], head_hash, acc) do
    %{signature_or_message: signature,
     address: address,
     value: value,  # integer type
     obsolete_tag: obsolete_tag,
     timestamp: timestamp,  # integer type
     current_index: current_index,
     last_index: last_index, # integer type
     bundle: bundle_hash,
     trunk: trunk,
     branch: branch,
     tag: tag,
     attachment_timestamp: atime,  # integer type
     attachment_timestamp_lower: alower,  # integer type
     attachment_timestamp_upper: aupper,  # integer type
     nonce: nonce,
     hash: hash,
     snapshot_index: snapshot_index  # integer type
     } = tx
     address_label = address_label?(value)
     tx_label = tx_label?(current_index,last_index)
    {:ok, b_a_r_qf, b_a_r_qs} =
      bundle_address_row_query(bundle_hash, address_label, timestamp,
      current_index, address, value, obsolete_tag, signature, last_index)
    {:ok, b_t_r_qf, b_t_r_qs} =
      bundle_tx_row_query(bundle_hash,tx_label,timestamp,current_index,
        head_hash,address,snapshot_index,hash,nonce,tag,trunk,branch,
        atime,alower,aupper)
    {:ok, e_a_r_qf, e_a_r_qs} =
      edge_address_row_query(zero_value?, address, address_label, timestamp, bundle_hash,
      value, current_index, tx_label, last_index)
    {:ok, e_t_r_qf, e_t_r_qs} =
      edge_tx_row_query(hash, tx_label, timestamp, bundle_hash, head_hash,
      current_index, address_label, last_index, snapshot_index)
      # v1, ts, v2, ix, el, lx
    zero_value_qs = zero_value_address_row_query(zero_value?, address, timestamp, bundle_hash,
      current_index, tx_label, last_index)
    acc = zero_value_qs ++ [{b_a_r_qf, b_a_r_qs},{b_t_r_qf, b_t_r_qs},{e_a_r_qf, e_a_r_qs},
      {e_t_r_qf, e_t_r_qs}] ++ acc
    _queries(zero_value?,rest, head_hash, acc)
  end

  defp _queries(_,[],_,acc) do
    acc
  end

  # function which create a query for address row in bundle table..
  defp bundle_address_row_query(bh, lb, ts, ix, addr, value, otag, s_o_m, lx) do
    {Tangle, Bundle}
    |> cql(@bundle_cql_for_address_row)
    |> type(:insert)
    |> values([
      {:blob, bh},
      {:tinyint, lb},
      {:varint, ts},
      {:varint, ix},
      {:blob, "addr"},
      {:blob, addr},
      {:varint, value},
      {:blob, otag},
      {:blob, s_o_m},
      {:varint, lx}])
    |> pk([bh: bh])
    |> prepare?(true)
    |> Execute.logged()
  end

  # function which create a query for tx row in bundle table..
  defp bundle_tx_row_query(bh,lb,ts,ix,head_hash,addr,
    snapshot_index,hash,nonce,tag,trunk,branch,
    attachment_timestamp,attachment_timestamp_lower,
    attachment_timestamp_upper) do
    {Tangle, Bundle}
    |> cql(@bundle_cql_for_tx_row)
    |> type(:insert)
    |> values([
      {:blob, bh},
      {:tinyint, lb},
      {:varint, ts},
      {:varint, ix},
      {:blob, head_hash},
      {:blob, addr},
      {:varint, snapshot_index},
      {:blob, hash},
      {:blob, nonce},
      {:blob, tag},
      {:blob, trunk},
      {:blob, branch},
      {:varint, attachment_timestamp},
      {:varint, attachment_timestamp_lower},
      {:varint, attachment_timestamp_upper}])
    |> pk([bh: bh])
    |> prepare?(true)
    |> Execute.logged()
  end


  # function which create a query for address row in zero_value table if true..
  # and return the query state.
  defp zero_value_address_row_query(true, v1, ts, v2, ix, el, lx) do
    # v1 blob, yy smallint, mm smallint, ts varint, v2 blob, ix varint, el varint, lx varint
    %{year: yy, month: mm} = DateTime.from_unix!(ts)
    {:ok, qf, qs} =
      {Tangle, ZeroValue}
      |> cql(@zero_value_cql_for_address_row)
      |> type(:insert)
      |> values([
        {:blob, v1},
        {:smallint, yy},
        {:smallint, mm},
        {:varint, ts},
        {:blob, v2},
        {:varint, ix},
        {:varint, el},
        {:varint, lx}])
      |> pk([v1: v1, yy: yy, mm: mm])
      |> prepare?(true)
      |> Execute.logged()
    [{qf, qs}]
  end


  # function return empty list as the bundle doesn't require zero_value query.
  defp zero_value_address_row_query(_, _, _, _, _, _, _) do
    []
  end

  defp edge_address_row_query(true, v1, _, ts, _, _, _, _, lx) do
    {Tangle, Edge}
    |> cql(@edge_hint_cql_for_address_row)
    |> type(:insert)
    |> values([
      {:blob, v1},
      {:varint, ts}, # NOTE timestamp as extralabel el.
      {:varint, lx}])
    |> pk([v1: v1])
    |> prepare?(true)
    |> Execute.logged()
  end

  defp edge_address_row_query(_, v1, lb, ts, v2, ex, ix, el, lx) do
    {Tangle, Edge}
    |> cql(@edge_cql_for_address_row)
    |> type(:insert)
    |> values([
      {:blob, v1},
      {:tinyint, lb},
      {:varint, ts},
      {:blob, v2},
      {:varint, ex}, # NOTE: this is blob but the scylla will convert varint to blob.
      {:varint, ix}, #
      {:varint, el},
      {:varint, lx}])
    |> pk([v1: v1])
    |> prepare?(true)
    |> Execute.logged()
  end

  defp edge_tx_row_query(v1, lb, ts, v2, ex, ix, el, lx, sx) do
    {Tangle, Edge}
    |> cql(@edge_cql_for_tx_row)
    |> type(:insert)
    |> values([
      {:blob, v1},
      {:tinyint, lb},
      {:varint, ts},
      {:blob, v2},
      {:blob, ex}, # this is blob.
      {:varint, ix},
      {:varint, el},
      {:varint, lx},
      {:varint, sx}])
    |> pk([v1: v1])
    |> prepare?(true)
    |> Execute.logged()
  end

  defp address_label?(value) when value >= 0 do
    # output in tinyint form.
    10
  end

  defp address_label?(_) do
    # input in tinyint form.
    20
  end

  defp tx_label?(ix,lx) when ix == lx do
    # headhash in tinyint form.
    40
  end

  defp tx_label?(_,_x) do
    # txhash in tinyint form.
    30
  end


end
