defmodule Broker.Collector.Inserter do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor
  alias Broker.Collector.Inserter.Helper
  @doc """
    starting bundle inserter
  """
  @spec start_link(list) :: tuple
  def start_link(bundle) do
    # in order to insert the bundle to the database,
    # we have to distinguish between value bundle and zero value bundle,
    # to know the required write queries.
    state =
      if Enum.all?(bundle, fn(%{value: value}) -> value == 0 end) do
        # it's zero_value bundle.
        {:zero_value, bundle}
      else
        # it's value bundle.
        {:value, bundle}
      end
    GenServer.start(__MODULE__, state)
  end

  @doc """
    This function initate the Inserter with zero_value bundle state
  """
  @spec init(tuple) :: tuple
  def init({:zero_value, bundle}) do
    # create zero_value bundle queries.
    Process.send_after(self(), :check, 10000)
    state = Map.put(Helper.queries(true, bundle)
    |> Enum.into(%{}), :bundle, bundle)
    {:ok, state}
  end

  @doc """
    This function initate Inserter with value bundle state
  """
  @spec init(tuple) :: tuple
  def init({:value, bundle}) do
    # create value bundle queries.
    Process.send_after(self(), :check, 10000)
    state = Map.put(Helper.queries(false, bundle)
    |> Enum.into(%{}), :bundle, bundle)
    {:ok, state}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:full, qf, buffer}, state) do
    # first we fetch the query state from the state using the qf key.
    case Protocol.decode_full(buffer, state[qf]) do
      :void when map_size(state) == 2 ->
        # we stop the worker as no further responses are expected.
        {:stop,:normal, Map.delete(state, qf)}
      :void ->
        # we proceed(noreply) the worker.
        {:noreply, Map.delete(state, qf)}
      err? ->
        # TODO: retry logic for the whole bundle.
        # (we stop/kill the worker to make the supervisor restart it)
        IO.inspect(err?)
        {:noreply, state}
    end
  end

  def handle_info(:check, state) do
    size = map_size(state)
    if size > 1 do
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
    This match unsuccessful send requests.
    # TODO: retry logic, (retry to re-insert the whole bundle)
  """
  def handle_cast({:send?, _, status}, %{from: from} = state) do
    {:stop, :normal, state}
  end

  def logged(query) do
    Execute.logged(query)
  end

end
