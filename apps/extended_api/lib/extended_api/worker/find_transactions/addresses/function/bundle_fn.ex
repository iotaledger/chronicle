defmodule ExtendedApi.Worker.FindTransactions.Addresses.BundleFn do

  @moduledoc """
    This module hold the function that is going to compute
    the bundle query result.
    the row will hold the transaction_hash..
  """

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
