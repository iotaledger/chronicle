defmodule ExtendedApi.Worker.FindTransactions.Addresses do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.FindTransactions.Addresses.Helper
  import ExtendedApi.Worker.Helper

  @doc """
    This function start FindTransactions.Bundles worker.
  """
  @spec start_link() :: tuple
  def start_link() do
    GenServer.start_link(__MODULE__, %{})
  end

  @doc """
    This function initate FindTransactions.Addresses worker
  """
  @spec init(map) :: tuple
  def init(state) do
    {:ok, state}
  end

  @doc """
    This function handle the call from the processor which
    started this processor, it stores a from reference
    and block on the caller
  """
  @spec handle_call(tuple, tuple, map) :: tuple
  def handle_call({:find_transactions, addresses}, from, state) do
    send(self(), {:addresses, addresses})
    state_map = Map.put(state, :from, from)
    {:noreply, state_map}
  end

  @doc """
    by looping through the addresses list and create edge query
    for each address, and it will break the api call
    if any interrupt occur.
  """
  @spec handle_info(tuple, map) :: tuple
  def handle_info({:addresses, addresses}, %{from: from} = state_map) do
    # create and send queries to scyllaDB.
    # there is no need to send queries to edge table,
    # because we already have the bundle_hashes.
    case Helper.queries(bundle_hashes, state_map) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        # we break, thus we should return error to client
        # before breaking.
        reply(from, {:error, reason})
        {:stop, :normal, bundle_hashes}
    end
  end

  @doc """
    Await function, it will be invoked only by the processor
    which start_link this processor to fetch the result.
  """
  @spec await(pid, list, integer) :: term
  def await(pid, bundle_hashes, timeout \\ :infinity) do
    GenServer.call(pid, {:find_transactions, bundle_hashes}, timeout)
  end

end
