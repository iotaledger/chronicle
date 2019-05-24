defmodule ExtendedApi.Worker.GetTrytes.BundleFn do

 # TODO: add specs

  @doc """
    This function handle address's row and creates map
    with the fixed_fields( which doesn't change after new
    reattachment)

    # NOTE: the result of this function is passed as acc
    to the next row
  """
  def construct(bh,el,_tx_lb,ts,ix,lx,ex,_ref, [{:lb, lb},{:va, address},{:a, value},{:c, _},{:d, otag},{:e, sigfrag}|_] = row, acc) when lb == el do
    _fixed_fields(bh,otag,ts,ix,lx,address,value,sigfrag)
  end
  @doc """
    This function handle transaction's row and creates map
    with the dynamic_fields(which change every new reattachment),
    it merges the transaction's row with address's row.
  """
  def construct(_bh,_el,_tx_lb,_ts,_ix,_lx,_ex,_ref, row, acc) when is_map(acc) do
    [{:lb, _},{:va, _},{:a, sx},{:c, nonce},
      {:d, tag},{:e, trunk},{:f, branch},
      {:g, a_stamp},{:h, upper},{:i, lower}] = row
    _dynamic_fields(sx,tag,nonce,trunk,branch,a_stamp,lower,upper)
    |> merge?(acc)
  end

   # this creates map which contain all the fixed fields
  defp _fixed_fields(bh,otag,ts,ix,lx,address,value,sigfrag) do
    %{
      value: value,
      bundle: bh,
      address: address,
      timestamp: ts,
      lastIndex: lx,
      obsoleteTag: otag,
      currentIndex: ix,
      signatureMessageFragment: sigfrag
    }
  end

   # this creates map which contain all the dynamic fields
   # NOTE: dynamic fields are the fields thats changes
   # with new reattachment. except snapshotIndex is an extra field.

  defp _dynamic_fields(sx,tag,nonce,trunk,branch,a_stamp,lower,upper) do
    %{
      tag: tag,
      nonce: nonce,
      snapshotIndex: sx,
      trunkTransaction: trunk,
      branchTransaction: branch,
      attachmentTimestamp: a_stamp,
      attachmentTimestampLowerBound: lower,
      attachmentTimestampUpperBound: upper
    }
  end

  # this merge the two rows(address's row, transaction's row)
  defp merge?(map,acc) when is_map(acc) do
    Map.merge(map, acc)
  end

  defp merge?(_,acc) do
    raise "merge? is not map bundleFn #{inspect acc}"
  end

end
