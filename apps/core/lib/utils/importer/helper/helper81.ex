defmodule Core.Utils.Importer.Helper81 do

  import OverDB.Builder.Query
  alias Core.DataModel.{Keyspace.Tangle, Table.Bundle, Table.Edge, Table.ZeroValue}
  alias Core.Utils.Importer.Worker81
  @bundle_cql_for_address_row "INSERT INTO tangle.bundle (bh,lb,ts,ix,id,va,a,d,e,g) VALUES (?,?,?,?,?,?,?,?,?,?)"
  @bundle_cql_for_tx_row "INSERT INTO tangle.bundle (bh,lb,ts,ix,id,va,a,b,c,e,f) VALUES (?,30,?,?,?,?,?,?,?,?,?)"
  @edge_cql_for_address_row "INSERT INTO tangle.edge (v1,lb,ts,v2,ex,ix,el,lx) VALUES (?,?,?,?,varintAsBlob(?),?,30,?)"
  @edge_cql_for_tx_row "INSERT INTO tangle.edge (v1,lb,ts,v2,ex,ix,el,lx,sx) VALUES (?,30,?,?,?,?,?,?,?)"
  @edge_cql_for_tx_nil_row "INSERT INTO tangle.edge (v1,lb,ts,v2,ex,ix,el,lx) VALUES (?,30,?,?,?,?,?,?)"
  @edge_hint_cql_for_address_row "INSERT INTO tangle.edge (v1,lb,ts,v2,ex,ix) VALUES (?,60,0,varintAsBlob(0),varintAsBlob(0),0)"
  @edge_cql_for_approve_row "INSERT INTO tangle.edge (v1,lb,ts,v2,ex,ix,lx) VALUES (?,?,?,?,?,?,?)"
  @zero_value_cql_for_address_row "INSERT INTO tangle.zero_value (v1,yy,mm,lb,ts,v2,ex,ix,el,lx) VALUES (?,?,?,10,?,?,varintAsBlob(0),?,30,?)"


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
     nonce: nonce,
     hash: hash,
     snapshot_index: snapshot_index  # integer type or nil
     } = tx
     address_label = address_label?(value)
    {:ok, b_a_r_qf, b_a_r_qs} =
      bundle_address_row_query(bundle_hash, address_label, timestamp,
      current_index, address, value, obsolete_tag, signature, last_index)
    {:ok, b_t_r_qf, b_t_r_qs} =
      bundle_tx_row_query(bundle_hash,timestamp,current_index,
        head_hash,address,snapshot_index,hash,nonce,trunk,branch)
    {:ok, e_a_r_qf, e_a_r_qs} =
      edge_address_row_query(zero_value?, address, address_label, timestamp, bundle_hash,
      value, current_index, last_index)
    {:ok, e_t_r_qf, e_t_r_qs} =
      edge_tx_row_query(hash, timestamp, bundle_hash, head_hash,
      current_index, address_label, last_index, snapshot_index)
    # extract yy,mm from timestamp
    %{year: yy, month: mm} =
      try do
        DateTime.from_unix!(timestamp)
      rescue
        e in ArgumentError ->
          %ArgumentError{message: <<"invalid Unix time ", timestamp::10-bytes, _::binary>>} = e
          String.to_integer(timestamp) |> DateTime.from_unix!()
      end
      # v1, ts,yy,mm, v2, ix, lx
    zero_value_qs = zero_value_address_row_query(zero_value?, address,timestamp,yy,mm, bundle_hash,
      current_index, last_index)
    # approve rows related write queries
    approves_qs = if current_index == last_index do
      # 50 is a trunk and 51 is branch.
      [edge_approve_row_query(trunk, 50, timestamp, bundle_hash, head_hash, current_index, last_index),
       edge_approve_row_query(branch, 51, timestamp, bundle_hash, head_hash, current_index, last_index)]
    else
      [edge_approve_row_query(branch, 51, timestamp, bundle_hash, head_hash, current_index, last_index)]
    end

    acc = approves_qs ++ zero_value_qs ++ [{b_a_r_qf, b_a_r_qs},{b_t_r_qf, b_t_r_qs},{e_a_r_qf, e_a_r_qs},
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
    |> assign_query()
    |> Worker81.logged()
  end

  # function which create a query for tx row in bundle table
  # for transactions of sn_trytes(with snapshot_index != nil/snapshot_index is integer >= 0 )..
  defp bundle_tx_row_query(bh,ts,ix,head_hash,addr,
    snapshot_index,hash,nonce,trunk,branch) when is_integer(snapshot_index) do
    {Tangle, Bundle}
    |> cql(@bundle_cql_for_tx_row)
    |> type(:insert)
    |> values([
      {:blob, bh},
      {:varint, ts},
      {:varint, ix},
      {:blob, head_hash},
      {:blob, addr},
      {:varint, snapshot_index},
      {:blob, hash},
      {:blob, nonce},
      {:blob, trunk},
      {:blob, branch}])
    |> pk([bh: bh])
    |> prepare?(true)
    |> assign_query()
    |> Worker81.logged()
  end

  # function which create a query for address row in zero_value table if true..
  # and return the query state.
  defp zero_value_address_row_query(true, v1, ts,yy,mm, v2, ix, lx) do
    # v1 blob, yy smallint, mm smallint, ts varint, v2 blob, ix varint, lx varint
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
        {:varint, lx}])
      |> pk([v1: v1, yy: yy, mm: mm])
      |> prepare?(true)
      |> assign_query()
      |> Worker81.logged()
    [{qf, qs}]
  end


  # function return empty list as the bundle doesn't require zero_value query.
  defp zero_value_address_row_query(_,_,_,_,_, _, _, _) do
    []
  end

  defp edge_address_row_query(true, v1, _, _ts, _, _, _, _) do
    {Tangle, Edge}
    |> cql(@edge_hint_cql_for_address_row)
    |> type(:insert)
    |> values([
      {:blob, v1}])
    |> pk([v1: v1])
    |> prepare?(true)
    |> assign_query()
    |> Worker81.logged()
  end

  defp edge_address_row_query(_, v1, lb, ts, v2, ex, ix, lx) do
    {Tangle, Edge}
    |> cql(@edge_cql_for_address_row)
    |> type(:insert)
    |> values([
      {:blob, v1},
      {:tinyint, lb},
      {:varint, ts},
      {:blob, v2},
      {:varint, ex}, # NOTE: this is blob but the scylla will convert varint(value) to blob.
      {:varint, ix}, #
      {:varint, lx}])
    |> pk([v1: v1])
    |> prepare?(true)
    |> assign_query()
    |> Worker81.logged()
  end

  defp edge_tx_row_query(v1, ts, v2, ex, ix, el, lx, sx) when is_nil(sx) do
    {Tangle, Edge}
    |> cql(@edge_cql_for_tx_nil_row)
    |> type(:insert)
    |> values([
      {:blob, v1},
      {:varint, ts},
      {:blob, v2},
      {:blob, ex}, # this is blob.
      {:varint, ix},
      {:varint, el},
      {:varint, lx}])
    |> pk([v1: v1])
    |> prepare?(true)
    |> assign_query()
    |> Worker81.logged()
  end

  defp edge_tx_row_query(v1, ts, v2, ex, ix, el, lx, sx) when is_integer(sx) do
    {Tangle, Edge}
    |> cql(@edge_cql_for_tx_row)
    |> type(:insert)
    |> values([
      {:blob, v1},
      {:varint, ts},
      {:blob, v2},
      {:blob, ex}, # this is blob.
      {:varint, ix},
      {:varint, el},
      {:varint, lx},
      {:varint, sx}])
    |> pk([v1: v1])
    |> prepare?(true)
    |> assign_query()
    |> Worker81.logged()
  end

  defp edge_approve_row_query(v1,lb, ts, v2, ex, ix,lx) do
    {:ok, qf, qs} =
      cql({Tangle, Edge},@edge_cql_for_approve_row)
      |> type(:insert)
      |> values([
        {:blob, v1},
        {:tinyint, lb},
        {:varint, ts},
        {:blob, v2},
        {:blob, ex}, # this is blob.
        {:varint, ix},{:varint, lx}])
      |> pk([v1: v1])
      |> prepare?(true)
      |> assign_query()
      |> Worker81.logged()
    {qf,qs}
  end

  # this assign a query to the query in order to enable us for retry logic.
  defp assign_query(query) do
    assign(query,query: query)
  end

  defp address_label?(value) when value >= 0 do
    # output in tinyint form.
    10
  end

  defp address_label?(_) do
    # input in tinyint form.
    20
  end

end
