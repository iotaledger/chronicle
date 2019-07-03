defmodule ExtendedApi.Worker.FindTransactions.Hints.TagFn do

  @moduledoc """
    This module hold the function that is going to compute
    the tag query result.
    the row will hold the transaction_hash..
  """

  @doc """
    This function handles tag's rows.

    # NOTE: the result of this function is passed as acc
    to the next row.
  """
  @spec construct(list, list) :: list
  def construct([th: th], acc) do
    [th | acc]
  end

end
