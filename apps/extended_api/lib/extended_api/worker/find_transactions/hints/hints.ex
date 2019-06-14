defmodule ExtendedApi.Worker.FindTransactions.Hints do


  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.FindTransactions.Hints.Helper
  import ExtendedApi.Worker.Helper

  @zero_value_cql "SELECT ts,v2,ix,el FROM tangle.zero_value WHERE v1 = ? AND yy = ? AND mm = ? AND lb = 10"
  @bundle_cql "SELECT b FROM tangle.bundle WHERE bh = ? AND lb = ? AND ts = ? AND ix = ?"

  @doc """
    This function start FindTransactions.Hints worker.
  """
  @spec start_link() :: tuple
  def start_link() do
    GenServer.start_link(__MODULE__, %{})
  end


  @doc """
    This function initate FindTransactions.Hints worker
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
  def handle_call({:find_transactions, hints}, from, state) do
    send(self(), {:hints, hints})
    state = Map.put(state, :from, from)
    {:noreply, state}
  end

  @doc """
    by looping through the hints list and create zero_value query
    for each hint, and it will break the api call
    if any interrupt occur.
  """
  @spec handle_info(tuple, map) :: tuple
  def handle_info({:hints, hints}, %{from: from}) do
    # create and send queries to scyllaDB.
    case Helper.queries(hints, from) do
      {:ok, state} ->
        {:noreply, state}
      {:error, reason} ->
        # we break, thus we should return error to client
        # before breaking.
        reply(from, {:error, reason})
        {:stop, :normal, hints}
    end
  end

  @doc """
    Await function, it will be invoked only by the processor
    which start_link this processor to fetch the result.
  """
  @spec await(pid, list, integer) :: term
  def await(pid, hints, timeout \\ :infinity) do
    GenServer.call(pid, {:find_transactions, hints}, timeout)
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
    This function handles full response of a query from zero_value table.
  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:full, {:zero_value, qf}, buffer}, %{ref: ref, hashes: hashes_list, hints: hints_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for hint state[qf][:hint] at a given hint[:year] and hint[:month] are ready if paging_state = nil.
      # Please note we should create new hint with updated month/year only if paging_state = nil.
      # otherwise we keep same month/year and put paging_state.
      {%Compute{result: queries_states}, %{hint: hint} = query_state} when is_list(queries_states) ->
        # create/update hint
        hint = Helper.create_hint(hint, query_state[:paging_state])
        # we delete the query_state for qf as it's no longer needed.
        state = Map.delete(state, qf)
        # update ref to include the queries_states count-1.
        ref = length(queries_states) + ref-1
        case ref do
          0 ->
            # this indicates it's the last response
            # (or might be the first and last)
            # therefore we fulfil the API call.
            # First we fetch the from reference for the caller processor.
            from = state[:from] # from reference.
            # we reply with updated hashes_list(if any) and hints.
            reply(from, {:ok, hashes_list, [hint | hints_list]})
            # now we stop the worker.
            {:stop, :normal, state}
          _ ->
            # create new updated state
            state = %{Enum.into(queries_states, state) | ref: ref, hints: [hint | hints_list]}
            # return new state
            {:noreply, state}
        end
      # this is {:error, _} handler
      {%Compute{result: {:error, reason}}, _} ->
        # we break.
        from = state[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
      # this is unprepared error handler
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of bundle query.
        cql = @zero_value_cql
        # we delete cql from cache.
        FastGlobal.delete(cql)
        # fetch the address,opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, hint: hint} = query_state
        # we pass the hint,qf,opts as arguments to generate zero_value query.
        {ok?, _, _} = Helper.zero_value_query(hint, qf, opts)
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

  @doc """
    This function handles full response of a query from bundle table.
  """
  @spec handle_cast(tuple, tuple) :: tuple
  def handle_cast({:full, {:bundle, qf}, buffer}, %{ref: ref, hashes: hashes_list, hints: hints_list} = state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the transaction_hashes for bundle_hash
      # state[qf][:bundle_hash] at current_index
      # state[qf][:current_index] are ready.
      {%Compute{result: hashes}, %{has_more_pages: false}} ->
        bundle_case_has_more_pages_false(qf,ref,hashes,hashes_list,hints_list,state)
      # this indicates the half_hashes are in half state.
      # which mean there still more rows(transactions_hashes)
      # have to be fetch with further queries requests.
      {%Compute{result: half_hashes}, %{has_more_pages: true, paging_state: p_state}} ->
        IO.inspect("arg1")
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

  @doc false
  @spec bundle_case_has_more_pages_false(integer,integer,list,list,list,map) :: tuple
  defp bundle_case_has_more_pages_false(qf,ref,hashes,hashes_list,hints_list,state) do
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
        from = state[:from] # from reference.
        reply(from, {:ok, hashes ++ hashes_list, hints_list})
        # now we stop the worker.
        {:stop, :normal, state}
      _ ->
        # this indicates it's not the last response.
        # (mean there are other queries under progress.)
        # we preappend hashes with hashes_list.
        hashes_list = hashes  ++ hashes_list
        # create new updated state
        state = %{Map.delete(state, qf) | ref: ref, hashes: hashes_list}
        # return updated state.
        {:noreply, state}
    end
  end

end
