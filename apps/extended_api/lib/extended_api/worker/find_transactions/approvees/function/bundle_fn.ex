defmodule ExtendedApi.Worker.FindTransactions.Approvees.BundleFn do

  @doc """
    This function handles bundle_hash's rows.
    and fetch the hash from column b.

    # NOTE: the result of this function is passed as acc
    to the next row.
  """
  @spec get_hash(list, list) :: list
  def get_hash([b: b], acc) do
    [b | acc]
  end

end
