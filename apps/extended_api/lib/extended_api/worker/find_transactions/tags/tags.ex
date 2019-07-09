defmodule ExtendedApi.Worker.FindTransactions.Tags do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.FindTransactions.Tags.Helper
  import ExtendedApi.Worker.Helper, only: [ok?: 2, reply: 2]

  @edge_cql "SELECT el FROM tangle.edge WHERE v1 = ? AND lb = 70"

  @doc """
    This function starts FindTransactions.Tags worker.
  """
  @spec start_link() :: tuple
  def start_link() do
    GenServer.start_link(__MODULE__, %{})
  end

  @doc """
    This function initates FindTransactions.Tags worker.
  """
  @spec init(map) :: tuple
  def init(state) do
    {:ok, state}
  end

  @doc """
    This function handles the call from the processor which
    started this processor, it stores a from reference
    and block on the caller
  """
  @spec handle_call(tuple, tuple, map) :: tuple
  def handle_call({:find_transactions, tags}, from, state) do
    send(self(), {:tags, tags})
    state = Map.put(state, :from, from)
    {:noreply, state}
  end

  @doc """
    by looping through the tags list and create edge query
    for each tag, and it will break the api call
    if any interrupt occur.
  """
  @spec handle_info(tuple, map) :: tuple
  def handle_info({:tags, tags}, %{from: from} = state) do
    # create and send queries to scyllaDB.
    case Helper.queries(tags, state) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        # we break, thus we should return error to client
        # before breaking.
        reply(from, {:error, reason})
        {:stop, :normal, state}
    end
  end

  @doc """
    This function handles full response of a query from edge table.
  """
  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:full, {:edge, qf}, buffer}, %{ref: ref, hints: hints_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the hint for tag state[qf][:tag] is ready.
      {%Compute{result: hint}, %{has_more_pages: false}} ->
        # we delete the query_state for qf as it's no longer needed.
        state = Map.delete(state, qf)
        # update ref.
        ref = ref-1
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state[:from] # from reference.
            reply(from, {:ok, hint ++ hints_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            state = %{ state |
              ref: ref,
              hints: hint ++ hints_list
            }
            # return new state
            {:noreply, state}
        end
      {%Compute{}, %{has_more_pages: true, opts: opts, tag: tag, paging_state: p_state}} ->
        # we put the paging state in opts
        opts = Map.put(opts, :paging_state, p_state)
        # create a new edge query attached with paging_state in opts.
        {ok?, _, query_state} = Helper.edge_query(tag, qf, opts)
        # update state with new query_state.
        state = %{state | qf => query_state}
        # verfiy to proceed or break.
        ok?(ok?, state)
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of edge query.
        FastGlobal.delete(@edge_cql)
        # fetch the tag,opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, tag: tag} = query_state
        # we pass the tag,qf,opts as arguments to generate edge query.
        {ok?, _, _} = Helper.edge_query(tag, qf, opts)
        # verfiy to proceed or break.
        ok?(ok?, state)
      # remaining error handler. ( error, eg read_timeout, etc)
      %Error{reason: reason} ->
        # we break.
        from = state[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
    end
  end

  # stream handler functions
  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({call, {:edge, qf}, buffer}, state) when call in [:start, :stream] do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # NOTE: it's impossible to have tag hint with :start/:stream handler
    # as the tag hint is only one row, so we expected in end handler only.
    # now we decode the buffer using the query_state.
    %Ignore{state: query_state} = Protocol.decode_all(call,buffer,query_state)
    # update state with new query_state.
    state = %{state | qf => query_state}
    # return updated state.
    {:noreply, state}
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:end, {:edge, qf}, buffer}, %{ref: ref, hints: hints_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the hint for tag state[qf][:tag] is ready.
      {%Compute{result: hint}, %{has_more_pages: false}} ->
        # we delete the query_state for qf as it's no longer needed.
        state = Map.delete(state, qf)
        # update ref.
        ref = ref-1
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state[:from] # from reference.
            reply(from, {:ok, hint ++ hints_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            state = %{ state |
              ref: ref,
              hints: hint ++ hints_list
            }
            # return new state
            {:noreply, state}
        end
      {%Compute{}, %{has_more_pages: true, opts: opts, tag: tag, paging_state: p_state}} ->
        # we put the paging state in opts
        opts = Map.put(opts, :paging_state, p_state)
        # create a new edge query attached with paging_state in opts.
        {ok?, _, query_state} = Helper.edge_query(tag, qf, opts)
        # update state with new query_state.
        state = %{state | qf => query_state}
        # verfiy to proceed or break.
        ok?(ok?, state)
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
    This match unsuccessful send requests.for simplcity
    we are droping the API call in case off any
    unsuccessful send request.
  """
  def handle_cast({:send?, _, status}, %{from: from} = state) do
    reply(from, status)
    {:stop, :normal, state}
  end

  @doc """
    Await function, it will be invoked only by the processor
    which start_link this processor to fetch the result.
  """
  @spec await(pid, list, integer) :: term
  def await(pid, tags, timeout \\ :infinity) do
    GenServer.call(pid, {:find_transactions, tags}, timeout)
  end

  @doc """
    Execute Query function, it's public function used by helper
    module to execute queries.
  """
  @spec query(map) :: tuple
  def query(map) do
    Execute.query(map)
  end

end
