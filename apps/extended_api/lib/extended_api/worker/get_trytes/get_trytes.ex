defmodule ExtendedApi.Worker.GetTrytes do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.GetTrytes.Helper
  import ExtendedApi.Worker.Helper, only: [ok?: 2, reply: 2]

  @edge_cql "SELECT lb,ts,v2,ex,ix,el,lx FROM tangle.edge WHERE v1 = ? AND lb IN ?"
  @bundle_cql "SELECT lb,va,a,c,d,e,f,g,h,i FROM tangle.bundle WHERE bh = ? AND lb IN ? AND ts = ? AND ix = ? AND id IN ?"
  @doc """
    This function handing starting GetTrytes worker.
  """
  @spec start_link() :: tuple
  def start_link() do
    GenServer.start_link(__MODULE__, %{})
  end

  @doc """
    This function initate GetTrytes worker
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
  def handle_call({:get_trytes, hashes}, from, state) do
    send(self(), {:hashes, hashes})
    state = Map.put(state, :from, from)
    {:noreply, state}
  end

  @doc """
    by looping through the hashes and create edge query
    for each hash, and it will break the api call
    if any interrupt occur.
  """
  @spec handle_info(tuple, map) :: tuple
  def handle_info({:hashes, hashes}, state) do
    # create and send queries to scyllaDB
    # we start with queries that belong to edge table
    # to fetch the required information that enable us
    # later to fetch the trytes from bundle table.
    case Helper.queries(hashes, state) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        # we break, thus we should return error to client
        # before breaking.
        reply(state[:from], {:error, reason})
        {:stop, :normal, hashes}
    end
  end

  @doc """
    This function handle full response from edge table.
    a full response might be an error(read-timeout, unprepared),
    or most important a result.
    Result after decode_full function:
    - Compute%{result: []}
      Empty list indicates that there is no row for
      txhash in trytes_list at index qf.

    - Compute%{result: tuple}
      tuple indicates that there was a row for
      txhash in trytes_list at index qf, and
      also we have successfully created and send
      a bundle_query.

    Error after decode_full function:
    - %Error{reason: :unprepared}
      This indicates that scylla had evicted the prepared statement,
      thus a retry is required.

    - other %Error{} reasons are possible for instance, :read_timeout.
      we will treat those error with breaking the API call.

  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:full, {:edge, qf}, buffer}, %{ref: ref, trytes: trytes_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer, query_state) do
      # this the tuple result.
      {%Compute{result: {_, _} = result}, _query_state} ->
        case result do
          {:ok, query_state} ->
            # this mean the bundle query had been sent,
            # therefore we should update the state map for qf
            # with the new query_state.
            state = %{state | qf => query_state}
            {:noreply, state}
          error ->
            # NOTE: we might add retry business logic.
            # we break and respond.
            from = state[:from] # this the caller reference.
            reply(from, error)
            {:stop, :normal, state}
        end
      # empty result because the transaction in trytes_list at index(qf)
      # doesn't have a row in ScyllaDB Edge table only if has_more_pages:
      # false, thus no further actions should be taken for that transaction.
      # unless has_more_pages: true.
      {%Compute{result: []}, %{has_more_pages: false}} ->
        # we reduce the ref.
        # NOTE: we reduce the ref only when the cycle for hash
        # is complete, thus empty list means no further
        # responses are expected for hash at index qf.
        ref = ref-1
        # we check if this response is the last response.
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state[:from] # this the caller reference.
            # Now we reply with the current trytes_list.
            reply(from, {:ok, trytes_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # this indicates it's not the last response.
            # therefore we return the updated state.
            # we don't longer need the query_state for qf.
            state = %{Map.delete(state, qf) | ref: ref}
            # we return updated state.
            {:noreply, state}
        end
      # empty result because of Scylla,thus further query
      # is required as has_more_pages: true.
      {%Compute{result: []}, %{hash: hash, opts: opts, has_more_pages: true, paging_state: p_state}} ->
        # put paging_state in opts
        opts = Map.put(opts, :paging_state, p_state)
        # re-send edge_query with updated opts.
        {ok?, _,query_state} = Helper.edge_query(hash, qf, opts)
        # we update the query_state in worker state.
        state = %{state | qf => query_state}
        # verfiy to proceed or break.
        ok?(ok?, state)
      # this is unprepared error handler
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of edge query.
        cql = @edge_cql
        # we delete the %Prepared{} struct from cache
        # this mean any future edge queries should be
        # re-prepare the %Prepared{} struct.
        FastGlobal.delete(cql)
        # now we fetch the assigned hash/opts from the query_state
        # we are fetching the opts because it might be a response
        # belongs to a paging_request.
        %{hash: hash, opts: opts} = query_state
        # We resend the query..
        # we don't care about new generated query state neither
        # qf(query_reference) because they match the current ones.
        {ok?, _,_} = Helper.edge_query(hash, qf, opts)
        # verfiy to proceed or break.
        ok?(ok?, state)
      %Error{reason: reason} ->
        # we break and respond.
        from = state[:from] # this the caller reference.
        reply(from, {:error, reason})
        {:stop, :normal, state}
    end
  end

  @doc """
    This function handle full response from bundle table.
    a full response might be an error(read-timeout, unprepared),
    or most important a result.
    Result after decode_full function:
    - Compute%{result: []}
      Empty list indicates that there is no row in the bundle
      table for transaction at index(qf) in trytes_list, this
      could happen when the data consistency has not yet been
      reached as ScyllaDB is eventual consistency, or maybe the
      ZMQ feeder(future APP) is feeding the Database in
      parallel with getTrytes API request.

    - Compute%{result: map}, hash_more_pages: false
      this indicates a complete response(two rows
      address's row and transaction's row.) is ready
      where the map should hold all the required
      fields to build the transaction trytes.

      # NOTE: those assumptions has not yet been tested.
    - Compute%{result: map}, hash_more_pages: true, paging_state: ..
      this indicates the response contains only
      address's row, so we must send new bundle query request with
      the paging_state to recv the transaction's row..

    Error after decode_full function:
    - %Error{reason: :unprepared}
      This indicates that scylla had evicted the prepared statement,
      thus a retry is required.

    - other %Error{} reasons are possible for instance, :read_timeout.
      we will treat those errors with breaking the API call.

  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:full, {:bundle, qf}, buffer}, %{ref: ref, trytes: trytes_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the tx-object map for transaction at index(qf)
      # is ready.
      {%Compute{result: map}, %{has_more_pages: false}} ->
        # this reduce the ref.
        # NOTE: we reduce the ref only when the cycle for hash
        # is complete, thus empty list means no further
        # responses are expected for hash at index qf.
        ref = ref-1
        # we check if this response is the last response.
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            # TODO: we should convert the map to trytes before proceeding.
            trytes_list = List.replace_at(trytes_list, qf, map)
            from = state[:from] # from reference.
            reply(from, {:ok, trytes_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # this indicates it's not the last response.
            # we replace the trytes_list at index qf with
            # the map result
            # NOTE: we should convert the map to trytes before doing so
            trytes_list = List.replace_at(trytes_list, qf, map)
            # we don't longer need the query_state for qf.
            state = %{Map.delete(state, qf) | trytes: trytes_list, ref: ref}
            # we return the updated state.
            {:noreply, state}
        end
      # this indicates the tx-object is a half_map for transaction at index(qf)
      # # NOTE: half_map only hold the address's row computing result.
      {%Compute{result: half_map}, %{opts: opts, has_more_pages: true, paging_state: p_state}} ->
        # NOTE: half_map might be an empty map (%{}) or the address's row map.
        # create new bundle query to fetch the remaining row(transaction's row or even both rows.)
        # we add paging state to the opts.
        opts = Map.put(opts, :paging_state, p_state)
        # we pass the opts and half_map(acc) as arguments to generate bundle query with paging_state.
        {ok?, _, query_state} = Helper.bundle_query_from_opts_acc(opts, half_map)
        # we update query_state in state
        state = %{state | qf => query_state}
        # verfiy to proceed or break.
        ok?(ok?, state)
      # this is unprepared error handler
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of bundle query.
        cql = @bundle_cql
        FastGlobal.delete(cql)
        # fetch the opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, acc: acc} = query_state
        # we pass the opts as an argument to generate bundle query.
        {ok?, _, _} = Helper.bundle_query_from_opts_acc(opts,acc)
        # verfiy to proceed or break.
        ok?(ok?, state)
      %Error{reason: reason} ->
        # we break and respond.
        from = state[:from] # this the caller reference.
        reply(from, {:error, reason})
        {:stop, :normal, state}
    end
  end

  @doc """
    This function handle start buffer for a streaming buffer.
  """
  def handle_cast({call, {:edge, qf}, buffer}, state) when call in [:start, :stream] do
    # we are certain the response will be %ignore{} because we are
    # expecting to receive only one row from edge table, which should be
    # handled in the end handler and not :start neither :stream.
    %Ignore{state: query_state} = Protocol.decode_all(call,buffer, Map.get(state, qf))
    state = %{state | qf => query_state}
    # we return updated state.
    {:noreply, state}
  end

  def handle_cast({:end, {:edge, qf}, buffer}, state) do
    # we are certain the response will be %Compute%{}
    case Protocol.decode_end(buffer, Map.get(state, qf)) do
      {%Compute{result: {_, _} = result}, _} ->
        case result do
          {:ok, query_state} ->
            # this mean the bundle query had been sent,
            # therefore we should update the state map for qf
            # with the new query_state.
            state = %{state | qf => query_state}
            {:noreply, state}
          error ->
            # NOTE: we might add retry business logic.
            # we break and respond.
            from = state[:from] # this the caller reference.
            reply(from, error)
            {:stop, :normal, state}
        end
      {%Compute{result: []}, %{has_more_page: false}} ->
        # we reduce the ref.
        ref = state[:ref]-1
        # we check if this response is the last response.
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state[:from] # this the caller reference.
            # Now we reply with the current trytes_list.
            reply(from, {:ok, state[:trytes]})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # this indicates it's not the last response.
            # therefore we return the updated state.
            # we don't longer need the query_state for qf.
            state = %{Map.delete(state, qf) | ref: ref}
            # return updated state.
            {:noreply, state}
        end
      # empty result because of Scylla,thus further query
      # is required as has_more_pages: true.
      {%Compute{result: []}, %{hash: hash, opts: opts, has_more_pages: true, paging_state: p_state}} ->
        # put paging_state in opts
        opts = Map.put(opts, :paging_state, p_state)
        # re-send edge_query with updated opts.
        {ok?, _,query_state} = Helper.edge_query(hash, qf, opts)
        # we update the query_state in worker state.
        state = %{state | qf => query_state}
        # verfiy to proceed or break.
        ok?(ok?, state)
    end
  end

  def handle_cast({call, {:bundle, qf}, buffer}, state) when call in [:start, :stream] do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_all(call,buffer,query_state) do
      # this indicates the address's map for transaction at index(qf) is ready
      {%Compute{result: half_map}, query_state} when is_map(half_map) ->
        query_state = Map.put(query_state, :acc, half_map)
        {:noreply, %{state | qf => query_state}}
      %Ignore{state: query_state} ->
        {:noreply, %{state | qf => query_state}}
    end
  end

  def handle_cast({:end, {:bundle, qf}, buffer}, %{ref: ref, trytes: trytes_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_end(buffer,query_state) do
      # this indicates the tx-object map for transaction at index(qf) is ready.
      {%Compute{result: map}, %{has_more_pages: false}} when is_map(map)->
        # we reduce the ref.
        ref = ref-1
        # we check if this response is the last response.
        case ref do
          0 -> # last
            # TODO: we should convert the map to trytes before proceeding.
            trytes_list = List.replace_at(trytes_list, qf, map)
            from = state[:from] # from reference.
            reply(from, {:ok, trytes_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ -> # not the last
            # we replace the trytes_list at index qf with
            # the map result
            # TODO: we should convert the map to trytes before doing so
            trytes_list = List.replace_at(trytes_list, qf, map)
            # we return the updated state.
            state = %{Map.delete(state, qf) | ref: ref, trytes: trytes_list}
            {:noreply, state}
        end
      {%Compute{result: half_map}, %{opts: opts, has_more_pages: true, paging_state: p_state}} ->
        # NOTE: half_map might be an empty map (%{}) or the address's row map.
        # create new bundle query to fetch the remaining row(transaction's row or even both rows.)
        # we add paging state to the opts.
        opts = Map.put(opts, :paging_state, p_state)
        # we pass the opts and half_map(acc) as arguments to generate bundle query with paging_state.
        {ok?, _, query_state} = Helper.bundle_query_from_opts_acc(opts, half_map)
        # we update query_state in state
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
  def await(pid, hashes, timeout \\ :infinity) do
    GenServer.call(pid, {:get_trytes, hashes}, timeout)
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
