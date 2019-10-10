defmodule ExtendedApi.Worker.FindTransactions.Tags.EdgeFn do

  @moduledoc """
    This module hold the function that are going to compute
    the edge query result, it creates tag hint for the tag row.

    finally it returns
    hint as list, mean the tag hint is ready
  """

  @doc """
   This function handle the edge row.
  """
  @spec create_hint(binary, list, list) :: map
  def create_hint(tag, _, acc) do
    # create hint map
    {:ok, %{year: year, month: month} } = DateTime.now("Etc/UTC")
    # there is possibility for one hint only per tag.
    [%{tag: tag, year: year, month: month} | acc]
  end

end
