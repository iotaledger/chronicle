defmodule ExtendedApi.Worker.FindTransactions.Bundles.BundleFn do

  @doc """
    This function handles bundle_hash's rows.

    # NOTE: the result of this function is passed as acc
    to the next row.
  """
  @spec construct(list, list) :: list
  def construct([b: b], acc) do
    [b | acc]
  end


end
