defmodule ExtendedApi.Worker.FindTransactions.Bundles do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.FindTransactions.Bundles.Helper
  import ExtendedApi.Worker.Helper

  @bundle_cql "SELECT b FROM tangle.bundle WHERE bh = ? AND lb IN ?"
  @doc """
    This function start FindTransactions.Bundles worker.
  """
  @spec start_link() :: tuple
  def start_link() do
    GenServer.start_link(__MODULE__, %{})
  end

  @doc """
    This function initate FindTransactions.Bundles worker
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
  def handle_call({:find_transactions, bundle_hashes}, from, state) do
    send(self(), {:bundles, bundle_hashes})
    state = Map.put(state, :from, from)
    {:noreply, state}
  end

  @doc """
    by looping through the bundle_hashes list and create bundle query
    for each bundle-hash, and it will break the api call
    if any interrupt occur.
  """
  @spec handle_info(tuple, map) :: tuple
  def handle_info({:bundles, bundle_hashes}, state) do
    # create and send queries to scyllaDB.
    # there is no need to send queries to edge table,
    # because we already have the bundle_hashes.
    case Helper.queries(bundle_hashes, state) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        # we break, thus we should return error to client
        # before breaking.
        reply(state[:from], {:error, reason})
        {:stop, :normal, state}
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
    This function handles full response of a query from bundle table.
  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:full, {:bundle, qf}, buffer}, %{ref: ref, hashes: hashes_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the transaction_hashes for bundle_hash state[qf][:bundle_hash]
      # are ready, also it's possible the result: hashes is an empty list, (eg bundle_hash
      # doesn't exist at all, or it's invalid.)
      {%Compute{result: hashes}, %{has_more_pages: false}} ->
        # we reduce the ref only when the cycle for bundle_hash
        # is complete, thus empty list means no further
        # responses are expected for state[qf][:bundle_hash].
        ref = ref-1
        # we check if this response is the last response.
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state[:from] # from reference.
            reply(from, {:ok, hashes ++ hashes_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # this indicates it's not the last response.
            # (mean there are other bundle_hashes queries
            # under progress.)
            # We only append the hashes to hashes_list.
            # We don't longer need the query_state for qf.
            # we return the updated state.
            state = %{
              Map.delete(state, qf) |
              ref: ref,
              hashes: hashes ++ hashes_list
              }
            {:noreply, state}
        end
      # this indicates the full transaction_hashes for bundle_hash
      # state[qf][:bundle_hash] are not completely ready yet.
      {%Compute{result: half_hashes}, %{has_more_pages: true, paging_state: p_state}} ->
        # create a new bundle query attached with paging_state
        # to fetch the remaining rows(bundle_hashe's rows)
        # Fetch opts, bundle_hash to add them for the query request.
        %{opts: opts, bundle_hash: bh} = query_state
        # Adding paging_state to the opts.
        opts = Map.put(opts, :paging_state, p_state)
        # we pass the bundle_hash, query_ref, opts as arguments
        # to generate bundle query with paging_state.
        {ok?, _, query_state} = Helper.bundle_query(bh, qf, opts)
        # we update query_state and hashes in state
        state = %{
          state |
          :hashes => half_hashes ++ hashes_list,
          qf => query_state
          }
        # verfiy to proceed or break.
        ok?(ok?, state)
      # this is unprepared error handler
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of bundle query.
        cql = @bundle_cql
        FastGlobal.delete(cql)
        # fetch the bh,opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, bundle_hash: bh} = query_state
        # we pass the bh,qf,opts as argument to generate bundle query.
        {ok?, _, _} = Helper.bundle_query(bh, qf, opts)
        # verfiy to proceed or break.
        ok?(ok?, state)
      # remaining error handler. ( error, eg read_timeout, etc)
      %Error{reason: reason} ->
        # we break and respond.
        from = state[:from] # this the caller reference.
        reply(from, {:error, reason})
        {:stop, :normal, state}
    end
  end

  def handle_cast({call, {:bundle, qf}, buffer}, state) when call in [:start, :stream] do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_all(call,buffer,query_state) do
      # this indicates some transaction hashes for bundle hash
      # state[qf][:bundle_hash] are might be ready
      {%Compute{result: half_hashes}, query_state} ->
        state = %{
          state |
          :hashes => half_hashes ++ state[:hashes],
          qf => query_state
          }
        {:noreply, state}
      %Ignore{state: query_state} ->
        state = %{state | qf => query_state}
        {:noreply, state}
    end
  end

  def handle_cast({:end, {:bundle, qf}, buffer}, %{ref: ref, hashes: hashes_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_end(buffer,query_state) do
      # this indicates transaction hashes for bundle hash
      # state[qf][:bundle_hash] are ready.
      {%Compute{result: hashes}, %{has_more_pages: false}} ->
        # we reduce the ref.
        ref = ref-1
        # we check if this response is the last response.
        case ref do
          0 ->
            # last response.
            from = state[:from] # from reference.
            reply(from, {:ok, hashes ++ hashes_list})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # this indicates it's not the last response.
            # (mean there are other bundle_hashes queries
            # under progress.)
            # We only append the hashes to hashes_list.
            # We don't longer need the query_state for qf.
            state = %{
              Map.delete(state, qf) |
              ref: ref,
              hashes: hashes++hashes_list
            }
            # we return the updated state.
            {:noreply, state}
        end
      {%Compute{result: half_hashes}, %{has_more_pages: true, paging_state: p_state}} ->
        # create a new bundle query attached with paging_state
        # to fetch the remaining rows(bundle_hashe's rows)
        # Fetch opts, bundle_hash to add them for the query request.
        %{opts: opts, bundle_hash: bh} = query_state
        # Adding paging_state to the opts.
        opts = Map.put(opts, :paging_state, p_state)
        # we pass the bundle_hash, query_ref, opts as arguments
        # to generate bundle query with paging_state.
        {ok?, _, query_state} = Helper.bundle_query(bh, qf, opts)
        # we update query_state in state
        state = %{
          state |
          qf => query_state,
          :hashes => half_hashes++hashes_list
        }
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

end
