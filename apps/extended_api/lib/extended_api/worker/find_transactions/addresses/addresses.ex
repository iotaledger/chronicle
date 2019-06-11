defmodule ExtendedApi.Worker.FindTransactions.Addresses do

  # NOTE:  WIP

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.FindTransactions.Addresses.Helper
  import ExtendedApi.Worker.Helper

  @doc """
    This function start FindTransactions.Addresses worker.
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
    case Helper.queries(addresses, state_map) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        # we break, thus we should return error to client
        # before breaking.
        reply(from, {:error, reason})
        {:stop, :normal, addresses}
    end
  end

  @doc """
    Await function, it will be invoked only by the processor
    which start_link this processor to fetch the result.
  """
  @spec await(pid, list, integer) :: term
  def await(pid, addresses, timeout \\ :infinity) do
    GenServer.call(pid, {:find_transactions, addresses}, timeout)
  end

  @doc """
    Execute Query function, it's public function used by helper
    module to execute queries.
  """
  @spec query(map) :: tuple
  def query(map) do
    Execute.query(map)
  end

  # handler functions

  @doc """
    This function handles full response of a query from edge table.
  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:full, {:edge, qf}, buffer}, {{ref, {hashes_list, hints_list}}, state_map} = state) do
    # first we fetch the query state from the state_map using the qf key.
    query_state = Map.get(state_map, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for address state_map[qf][:address] are ready.
      {%Compute{result: {:ok, queries_states, hints}}, %{has_more_pages: false}} ->
        # we delete the query_state for qf as it's no longer needed.
        state_map = Map.delete(state_map, qf)
        # update ref to include the queries_states count.
        ref = map_size(queries_states) + ref
        # merge queries_states map with worker's state_map
        state_map = Map.merge(state_map, queries_states)
        # update hints_list with the new hints(if any)
        hints_list = hints ++ hints_list
        # create new updated state
        state = {{ref, {hashes_list, hints_list}}, state_map}
        # return new state
        {:noreply, state}
        # this indicates the queries_states for bundle queries which select transaction_hashes
        # for address state_map[qf][:address] are not completely ready, because
        # there still remaining address's rows should be retrieved using paging_state.
      {%Compute{result: {:ok, queries_states, hints}}, %{has_more_pages: true, paging_state: p_state}} ->
        # create a new edge query attached with paging_state
        # to fetch the remaining rows(address's rows)
        # Fetch opts, address to add them for the query request.
        %{opts: opts, address: address} = query_state
        # Adding paging_state to the opts.
        opts = Map.put(opts, :paging_state, p_state)
        # we pass the address, qf, opts as arguments
        # to generate edge query with paging_state.
        {ok?, _, query_state} = Helper.edge_query(address, qf, opts)
        # update ref to include the current queries_states count.
        ref = map_size(queries_states) + ref
        # update query_state in state_map
        state_map = %{state_map | qf => query_state}
        # merge queries_states map with worker's state_map
        state_map = Map.merge(state_map, queries_states)
        # update hints_list with the new hints(if any)
        hints_list = hints ++ hints_list
        # create new updated state
        state = {{ref, {hashes_list, hints_list}}, state_map}
        # verfiy to proceed or break.
        ok?(ok?, state_map, state)
      # this indicates no rows are found in edge table for state_map[qf][:address]
      {%Compute{result: []}, _} ->
        # we no longer need the query_state for qf, because it does complete
        # the cycle(no future bundle queries)
        state_map = Map.delete(state_map, qf)
        # we reduce ref.
        ref = ref-1
        # we check if this response is the last response.
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state_map[:from] # from reference.
            reply(from, {:ok, hashes_list, hints_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # this indicates it's not the last response.
            # (mean there are other addresses edge queries
            # under progress.
            # update state with state_map.
            state = put_elem(state, 1, state_map)
            {:noreply, state}
        end
      # this is {:error, _} handler
      {%Compute{result: {:error, reason}}, _} ->
        # we break.
        # we break and respond.
        from = state_map[:from] # this the caller reference.
        reply(from, {:error, reason})
        {:stop, :normal, state}
      # this is unprepared error handler
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of bundle query.
        cql = "SELECT * FROM tangle.edge WHERE v1 = ? AND lb IN ?"
        FastGlobal.delete(cql)
        # fetch the address,opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, address: address} = query_state
        # we pass the bh,qf,opts as argument to generate bundle query.
        {ok?, _, _} = Helper.edge_query(address, qf, opts)
        # verfiy to proceed or break.
        ok?(ok?, state_map, state)
      # remaining error handler. ( error, eg read_timeout, etc)
      %Error{reason: reason} ->
        # we break and respond.
        from = state_map[:from] # this the caller reference.
        reply(from, {:error, reason})
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
    This match unsuccessful send requests.for simplcity
    we are droping the API call in case off any
    unsuccessful send request.
  """
  def handle_cast({:send?, _, status}, {_, %{from: from}} = state) do
    reply(from, status)
    {:stop, :normal, state}
  end


end
