defmodule ExtendedApi.Worker.FindTransactions.Addresses do

  # NOTE:  require heavey testing.

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.FindTransactions.Addresses.Helper
  import ExtendedApi.Worker.Helper

  @edge_cql "SELECT * FROM tangle.edge WHERE v1 = ? AND lb IN ?"
  @bundle_cql "SELECT b FROM tangle.bundle WHERE bh = ? AND lb = ? AND ts = ? AND ix = ?"
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
      {%Compute{result: {:ok, queries_states, hint}}, %{has_more_pages: false}} ->
        edge_case_has_more_pages_false(queries_states,hint,qf,ref,state_map,hashes_list,hints_list,state)
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for address state_map[qf][:address] are not completely ready, because
      # there still remaining address's rows should be retrieved using paging_state.
      {%Compute{result: {:ok, queries_states, hint}}, %{has_more_pages: true, paging_state: p_state}} ->
        edge_case_has_more_pages_true(query_state, queries_states,hint,qf,ref,state_map,hashes_list,hints_list,p_state)
      # this is {:error, _} handler
      {%Compute{result: {:error, reason}}, _} ->
        # we break.
        from = state_map[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
      # this is unprepared error handler
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of bundle query.
        cql = @edge_cql
        FastGlobal.delete(cql)
        # fetch the address,opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, address: address} = query_state
        # we pass the address,qf,opts as arguments to generate edge query.
        {ok?, _, _} = Helper.edge_query(address, qf, opts)
        # verfiy to proceed or break.
        ok?(ok?, state_map, state)
      # remaining error handler. ( error, eg read_timeout, etc)
      %Error{reason: reason} ->
        # we break.
        from = state_map[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
    end
  end


  @doc """
    This function handles full response of a query from bundle table.
  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:full, {:bundle, qf}, buffer}, {{ref, {hashes_list, hints_list}}, state_map} = state) do
    # first we fetch the query state from the state_map using the qf key.
    query_state = Map.get(state_map, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the transaction_hashes for bundle_hash
      # state_map[qf][:bundle_hash] at current_index
      # state_map[qf][:current_index] are ready.
      {%Compute{result: hashes}, %{has_more_pages: false}} ->
        bundle_case_has_more_pages_false(qf, ref, state_map, hashes, hashes_list, hints_list, state)
      # this indicates the half_hashes are in half state.
      # which mean there still more rows(transactions_hashes)
      # have to be fetch with further queries requests.
      {%Compute{result: half_hashes}, %{has_more_pages: true, paging_state: p_state}} ->
        bundle_case_has_more_pages_true(p_state, query_state,qf,ref,state_map,half_hashes,hashes_list,hints_list)
      # this is unprepared error handler.
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of bundle query.
        cql = @bundle_cql
        FastGlobal.delete(cql)
        # fetch the address,opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, bundle_hash: bh, current_index: ix, label: el, timestamp: ts}
          = query_state
        # we pass the address,qf,opts as arguments to generate bundle query.
        {ok?, _, _query_state} = Helper.bundle_query(bh, el, ts, ix, opts)
        # verfiy to proceed or break.
        ok?(ok?, state_map, state)
      # remaining error handler. ( error, eg read_timeout, etc)
      %Error{reason: reason} ->
        # we break.
        from = state_map[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
    end
  end

  # stream handler functions
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({call, {:edge, qf}, buffer}, {{ref, response_state}, state_map} = state) when call in [:start, :stream] do
    # first we fetch the query state from the state_map using the qf key.
    query_state = Map.get(state_map, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_all(call,buffer,query_state) do
      # this indicates some queries_states initited by
      # state_map[qf][:address] rows are might be ready
      # NOTE: it's impossible to have hint with :start/:stream handler,
      {%Compute{result: {:ok, queries_states, _hint}}, query_state} ->
        # update ref to add the new queries_states
        ref = map_size(queries_states) + ref
        # update state_map
        state_map = Map.put(state_map, qf, query_state)
        # return updated state
        {:noreply, {{ref, response_state}, state_map}}
      {%Compute{result: {:error, reason}}, _} ->
        # we break.
        from = state_map[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
      %Ignore{state: query_state} ->
        state = put_elem(state, 1, Map.put(state_map, qf, query_state))
        {:noreply, state}
    end
  end

  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:end, {:edge, qf}, buffer}, {{ref, {hashes_list, hints_list}}, state_map} = state) do
    # first we fetch the query state from the state_map using the qf key.
    query_state = Map.get(state_map, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for address state_map[qf][:address] are ready.
      {%Compute{result: {:ok, queries_states, hint}}, %{has_more_pages: false}} ->
        edge_case_has_more_pages_false(queries_states,hint,qf,ref,state_map,hashes_list,hints_list,state)
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for address state_map[qf][:address] are not completely ready, because
      # there still remaining address's rows should be retrieved using paging_state.
      {%Compute{result: {:ok, queries_states, hint}}, %{has_more_pages: true, paging_state: p_state}} ->
        edge_case_has_more_pages_true(query_state, queries_states,hint,qf,ref,state_map,hashes_list,hints_list,p_state)
      # this is {:error, _} handler
      {%Compute{result: {:error, reason}}, _} ->
        # we break.
        from = state_map[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
    end
  end

  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({call, {:bundle, qf}, buffer}, {{ref, {hashes_list, _} = response_state}, state_map} = state) when call in [:start, :stream] do
    # first we fetch the query state from the state_map using the qf key.
    query_state = Map.get(state_map, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_all(call,buffer,query_state) do
      # this indicates some hashes for state_map[qf][:bundle_hash]
      # are might be ready
      {%Compute{result: hashes}, query_state} ->
        # add hashes to hashes_list
        hashes_list = hashes ++ hashes_list
        response_state = put_elem(response_state, 1, hashes_list)
        {:noreply, {{ref, response_state}, Map.put(state_map, qf, query_state)}}
      %Ignore{state: query_state} ->
        state = put_elem(state, 1, Map.put(state_map, qf, query_state))
        {:noreply, state}
    end
  end

  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:end, {:bundle, qf}, buffer}, {{ref, {hashes_list, hints_list}}, state_map} = state) do
    # first we fetch the query state from the state_map using the qf key.
    query_state = Map.get(state_map, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_end(buffer,query_state) do
      # this indicates the transaction_hashes for bundle_hash
      # state_map[qf][:bundle_hash] at current_index
      # state_map[qf][:current_index] are ready.
      {%Compute{result: hashes}, %{has_more_pages: false}} ->
        bundle_case_has_more_pages_false(qf, ref, state_map, hashes, hashes_list, hints_list, state)
      # this indicates the half_hashes are in half state.
      # which mean there still more rows(transactions_hashes)
      # have to be fetch with further queries requests.
      {%Compute{result: half_hashes}, %{has_more_pages: true, paging_state: p_state}} ->
        bundle_case_has_more_pages_true(p_state, query_state,qf,ref,state_map,half_hashes,hashes_list,hints_list)
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

  @doc false
  @spec bundle_case_has_more_pages_false(integer,integer,map,list,list,list,tuple) :: tuple
  defp bundle_case_has_more_pages_false(qf,ref,state_map,hashes,hashes_list,hints_list,state) do
    # hashes might be an empty list in rare situations(data consistency
    # between edge_table and bundle_table or between replicas).
    # we reduce ref as the cycle for qf completed.
    ref = ref-1
    # check if the last response.
    case ref do
      0 ->
        # this indicates it's the last response
        # (or might be the first and last)
        # therefore we fulfil the API call.
        # First we fetch the from reference for the caller processor.
        from = state_map[:from] # from reference.
        reply(from, {:ok, hashes ++ hashes_list, hints_list})
        # now we stop the worker.
        {:stop, :normal, state}
      _ ->
        # this indicates it's not the last response.
        # (mean there are other queries under progress.)
        # we preappend hashes with hashes_list.
        hashes_list = hashes  ++ hashes_list
        # no longer need query_state for qf in state_map.
        state_map = Map.delete(state_map, qf)
        # update worker's state.
        state = {{ref, {hashes_list, hints_list}}, state_map}
        # return updated state.
        {:noreply, state}
    end
  end

  @doc false
  @spec bundle_case_has_more_pages_true(binary, map, integer, integer, map, list, list, list) :: tuple
  defp bundle_case_has_more_pages_true(p_state,query_state,qf,ref,state_map,half_hashes,hashes_list,hints_list) do
    # pattern matching on opts,bh,ix,el,ts from the query_state,
    # as we need those to issue new bundle_query.
    %{opts: opts, bundle_hash: bh, current_index: ix, label: el, timestamp: ts}
      = query_state
    # add paging_state to opts
    opts = Map.put(opts, :paging_state, p_state)
    # we pass bh,el,ts,ix,opts as arguments
    # to generate bundle query with paging_state.
    {ok?, _, query_state} = Helper.bundle_query(bh, el, ts, ix, opts)
    # we preappend half_hashes with hashes_list.
    hashes_list = half_hashes  ++ hashes_list
    # update state
    state = {{ref, {hashes_list, hints_list}}, %{state_map | qf => query_state}}
    # verfiy to proceed or break.
    ok?(ok?, state_map, state)
  end

  @doc false
  @spec edge_case_has_more_pages_false(map, list, integer, integer, map, list,list,tuple) :: tuple
  defp edge_case_has_more_pages_false(queries_states,hint,qf,ref,state_map,hashes_list,hints_list,state) do
    # we delete the query_state for qf as it's no longer needed.
    state_map = Map.delete(state_map, qf)
    # update ref to include the queries_states count-1.
    ref = map_size(queries_states) + ref-1
    case ref do
      0 ->
        # this indicates it's the last response
        # (or might be the first and last)
        # therefore we fulfil the API call.
        # First we fetch the from reference for the caller processor.
        from = state_map[:from] # from reference.
        reply(from, {:ok, hashes_list, hint ++ hints_list})
        # now we stop the worker.
        {:stop, :normal, state}
      _ ->
        # merge queries_states map with worker's state_map
        state_map = Map.merge(state_map, queries_states)
        # update hints_list with the new hints(if any)
        hints_list = hint ++ hints_list
        # create new updated state
        state = {{ref, {hashes_list, hints_list}}, state_map}
        # return new state
        {:noreply, state}
    end
  end

  @doc false
  @spec edge_case_has_more_pages_true(map, map, binary, integer,integer,map,list,list,binary) :: tuple
  defp edge_case_has_more_pages_true(query_state, queries_states,hint,qf,ref,state_map,hashes_list,hints_list,p_state) do
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
    hints_list = hint ++ hints_list
    # create new updated state
    state = {{ref, {hashes_list, hints_list}}, state_map}
    # verfiy to proceed or break.
    ok?(ok?, state_map, state)
  end
  
end
