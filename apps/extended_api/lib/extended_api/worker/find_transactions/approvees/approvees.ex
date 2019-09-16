defmodule ExtendedApi.Worker.FindTransactions.Approvees do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.FindTransactions.Approvees.Helper
  import ExtendedApi.Worker.Helper, only: [ok?: 2, reply: 2]

  @edge_cql "SELECT v2,ts,ex,ix,lx FROM tangle.edge WHERE v1 = ? AND lb = 50"
  @point_tx_bundle_cql "SELECT b FROM tangle.bundle WHERE bh = ? AND lb = 30 AND ts = ? AND ix = ? AND id = ?"

  @doc """
    This function starts FindTransactions.Approvees worker.
  """
  @spec start_link() :: tuple
  def start_link() do
    GenServer.start_link(__MODULE__, %{})
  end

  @doc """
    This function initates FindTransactions.Approvees worker.
  """
  @spec init(map) :: tuple
  def init(state) do
    {:ok, state}
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


  @doc """
    This function handles the call from the processor which
    started this processor, it stores a from reference
    and block on the caller
  """
  @spec handle_call(tuple, tuple, map) :: tuple
  def handle_call({:find_transactions, approvees}, from, state) do
    send(self(), {:approvees, approvees})
    state = Map.put(state, :from, from)
    {:noreply, state}
  end

  @doc """
    by looping through the approvees list and create edge query
    for each approve(tx-hash), and it will break the api call
    if any interrupt occur.
  """
  @spec handle_info(tuple, map) :: tuple
  def handle_info({:approvees, approvees}, %{from: from} = state) do
    # create and send queries to scyllaDB.
    case Helper.queries(approvees, state) do
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
    This function handle full/end response from edge table.
    a full response might be an error(read-timeout, unprepared),
    or most importantly a result.
  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({call, {:edge, qf}, buffer}, %{ref: ref, hashes: hashes_list} = state) when call in [:full, :end] do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_all(call, buffer, query_state) do
      # this handles both the computed and empty results when has_more_pages: false.
      {%Compute{result: %{hashes: hashes, queries_states: queries_states}}, %{has_more_pages: false}} ->
        # NOTE: hashes/queries_states might be empty lists.
        # therefore we should add the length of queries_states to ref.
        ref = length(queries_states) + ref-1
        # we check if this response is the last response.
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state[:from] # this the caller reference.
            # Now we reply with the updated hashes_list.
            reply(from, {:ok, hashes++hashes_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # this indicates it's not the last response.
            # therefore we return the updated state.
            # Merge the queries_states into state map,
            # we don't longer need the query_state for qf, that's why we are deleting it.
            state = %{Enum.into(queries_states, Map.delete(state, qf)) | hashes: hashes++hashes_list, ref: ref}
            # we return updated state.
            {:noreply, state}
        end
      # has_more_pages:true because of Scylla,thus further query is required.
      {%Compute{result: %{hashes: hashes, queries_states: queries_states}},
       %{approvee: approvee, opts: opts, has_more_pages: true, paging_state: p_state}} ->
        # add(if any) length(queries_states) to ref
        ref = length(queries_states) + ref
        # put paging_state in opts
        opts = Map.put(opts, :paging_state, p_state)
        # re-send edge_query with updated opts using the same qf.
        {ok?, _,query_state} = Helper.edge_query(approvee, qf, opts)
        # we update the query_state and hashes(if any) in worker state.
        state = %{Enum.into(queries_states,state) | qf => query_state, :hashes => hashes++hashes_list, :ref => ref}
        # verfiy to proceed or break.
        ok?(ok?, state)
      # error handler when edge_fn fails to send a bundle_query because of dead query engine.
      {%Compute{result: {:error, error}}, _} ->
        # we break and respond.
        from = state[:from] # this the caller reference.
        reply(from, {:error, error})
        {:stop, :normal, state}
      # this is unprepared error handler.
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of edge query.
        cql = @edge_cql
        FastGlobal.delete(cql)
        # now we fetch the assigned approvee/opts from the query_state
        # we are fetching the opts because it might be a response
        # belongs to a paging_request.
        %{approvee: approvee, opts: opts} = query_state
        # We resend the query..
        # we don't care about new generated query state neither
        # qf(query_reference) because they match the current one.
        {ok?, _,_} = Helper.edge_query(approvee, qf, opts)
        # verfiy to proceed or break.
        ok?(ok?, state)
      %Error{reason: reason} ->
        # we break and respond.
        from = state[:from] # this the caller reference.
        reply(from, {:error, reason})
        # stop the worker.
        {:stop, :normal, state}
    end
  end

  @doc """
    This function handle start buffer for a streaming buffer.
  """
  def handle_cast({call, {:edge, qf}, buffer}, %{ref: ref, hashes: hashes_list} = state) when call in [:start, :stream] do
    case Protocol.decode_all(call,buffer, Map.get(state, qf)) do
      {%Compute{result: %{hashes: hashes, queries_states: queries_states}},query_state} ->
        # update ref to include the length of queries_states(if any)
        ref = length(queries_states)+ref
        # update hashes_list(if any)
        hashes_list = hashes++hashes_list
        # update worker's state
        state = %{Enum.into(queries_states, state) | qf => query_state, :ref => ref, :hashes => hashes_list}
        # return updated state
        {:noreply, state}
      # error handler when edge_fn fails to send a bundle_query because of dead query engine.
      {%Compute{result: {:error, error}}, _} ->
        # we break and respond.
        from = state[:from] # this the caller reference.
        reply(from, {:error, error})
        # stop the worker.
        {:stop, :normal, state}
      %Ignore{state: query_state} ->
        state = %{state | qf => query_state}
        # we return updated state.
        {:noreply, state}
    end
  end

  @doc """
    # handles both full/end repsonses from bundle_table.
  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({call, {:bundle, qf}, buffer}, %{ref: ref, hashes: hashes_list} = state) when call in [:full, :end] do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the hashes for bundle_hash state[qf][:bh] are ready
      {%Compute{result: hashes}, %{has_more_pages: false}} ->
        # hashes might be an empty list in rare condiations,
        # when the data consistency is not yet acheived.
        # for example, chronicle_node(zmq app) might insert the bundle transactions
        # in bundle table with consistency level = 1, and concurrently we have worker(this worker)
        # requesting the random replica, which happen to not yet store the bundle.
        # reduce the ref.
        ref = ref-1
        # we check if this response is the last response.
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state[:from] # from reference.
            reply(from, {:ok, hashes++hashes_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # this indicates it's not the last response.
            # we don't longer need the query_state for qf.
            # update the state with new hashes and ref.
            state = %{Map.delete(state, qf) | hashes: hashes++hashes_list, ref: ref}
            # we return the updated state.
            {:noreply, state}
        end
      {%Compute{result: half_hashes},
       %{bh: bh, ix: ix, id: id, ts: ts, opts: opts, has_more_pages: true, paging_state: p_state}} ->
        # create new bundle query to fetch the remaining rows
        # we add paging state to the opts.
        opts = Map.put(opts, :paging_state, p_state)
        {ok?, _, query_state} = Helper.bundle_query(ix,bh,id,ts,qf,opts)
        # we update query_state and hashes in worker's state
        state = %{state | qf => query_state, :hashes => half_hashes++hashes_list}
        # verfiy to proceed or break.
        ok?(ok?, state)
      # this is unprepared error handler
      %Error{reason: :unprepared} ->
        %{bh: bh, ix: ix, id: id, ts: ts, opts: opts} = query_state
        # we use hardcoded cql statement of bundle query.
        cql = @point_tx_bundle_cql
        FastGlobal.delete(cql)
        # create new bundle_query.
        {ok?, _, _} = Helper.bundle_query(ix,bh,id,ts,qf,opts)
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
    This function handle start/stream buffer for a streaming buffer.
  """
  def handle_cast({call, {:bundle, qf}, buffer}, %{hashes: hashes_list} = state) when call in [:start, :stream] do
    case Protocol.decode_all(call,buffer, Map.get(state, qf)) do
      {%Compute{result: hashes}, query_state} ->
        # update hashes_list to include hashes(if any)
        hashes_list = hashes++hashes_list
        # update worker's state
        state = %{state | qf => query_state, :hashes => hashes_list}
        # return updated state
        {:noreply, state}
      %Ignore{state: query_state} ->
        state = %{state | qf => query_state}
        # we return updated state.
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
    This match unsuccessful send requests.for simplcity
    we are droping the API call in case off any
    unsuccessful send request.
  """
  def handle_cast({:send?, _, status}, %{from: from} = state) do
    reply(from, status)
    {:stop, :normal, state}
  end

end
