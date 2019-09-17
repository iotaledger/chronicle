defmodule Core.Utils.Importer.Worker27 do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor
  alias Core.Utils.Importer.Helper27
  require Logger
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
    state = Map.put(Helper27.queries(true, bundle)
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
    state = Map.put(Helper27.queries(false, bundle)
    |> Enum.into(%{}), :bundle, bundle)
    {:ok, state}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:full, qf, buffer}, state) do
    query_state = state[qf]
    # first we fetch the query state from the state using the qf key.
    case Protocol.decode_full(buffer, query_state) do
      :void when map_size(state) == 2 ->
        # we stop the worker as no further responses are expected.
        send(:importer, :finished_one)
        {:stop,:normal, Map.delete(state, qf)}
      :void ->
        # we proceed(noreply) the worker.
        {:noreply, Map.delete(state, qf)}
      %Error{reason: :unprepared} ->
        # retry the query which faild to get inserted due to unprepared statement
        # first we fetch the cql statement from the query_state
        %{query: %{cql: cql} = query} = query_state
        Logger.warn("retrying: #{qf}, query: #{inspect query}")
        # delete cql from the cache
        FastGlobal.delete(cql)
        # resend the query(logged)
        {:ok, _, _} = logged(query)
        # we return state as it's because we are retrying again.
        {:noreply, state}
      err? ->
        # those errors are likely due to write-timeout or scylladb related errors
        # TODO: handle them somehow.
        Logger.error("Error from worker27, #{inspect err?}")
        {:noreply, state}
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
  def handle_cast({:send?, _, _}, state) do
    {:stop, :normal, state}
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

  def logged(query) do
    Execute.logged(query)
  end

end
