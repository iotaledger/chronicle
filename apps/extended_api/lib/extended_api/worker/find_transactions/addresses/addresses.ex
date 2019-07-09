defmodule ExtendedApi.Worker.FindTransactions.Addresses do

  use GenServer
  use OverDB.Worker,
    executor: Core.Executor

  alias ExtendedApi.Worker.FindTransactions.Addresses.Helper
  import ExtendedApi.Worker.Helper

  @edge_cql "SELECT lb,el,ts,v2,ix FROM tangle.edge WHERE v1 = ? AND lb IN ?"
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
    state = Map.put(state, :from, from)
    {:noreply, state}
  end

  @doc """
    by looping through the addresses list and create edge query
    for each address, and it will break the api call
    if any interrupt occur.
  """
  @spec handle_info(tuple, map) :: tuple
  def handle_info({:addresses, addresses}, %{from: from} = state) do
    # create and send queries to scyllaDB.
    case Helper.queries(addresses, state) do
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
  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:full, {:edge, qf}, buffer}, state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for address state[qf][:address] are ready.
      {%Compute{result: result}, %{has_more_pages: false}} when is_map(result) ->
        edge_case_has_more_pages_false(qf, result, state)
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for address state[qf][:address] are not completely ready, because
      # there still remaining address's rows should be retrieved using paging_state.
      {%Compute{result: result}, query_state} when is_map(result) ->
        edge_case_has_more_pages_true(qf, result, query_state, state)
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
        cql = @edge_cql
        FastGlobal.delete(cql)
        # fetch the address,opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, address: address} = query_state
        # we pass the address,qf,opts as arguments to generate edge query.
        {ok?, _, _} = Helper.edge_query(address, qf, opts)
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
  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:full, {:bundle, qf}, buffer}, state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the transaction_hashes for bundle_hash
      # state[qf][:bundle_hash] at current_index
      # state[qf][:current_index] are ready.
      {%Compute{result: hashes}, %{has_more_pages: false}} ->
        bundle_case_has_more_pages_false(qf, hashes, state)
      # this indicates the half_hashes are in half state.
      # which mean there still more rows(transactions_hashes)
      # have to be fetch with further queries requests.
      {%Compute{result: half_hashes}, query_state} ->
        bundle_case_has_more_pages_true(Helper, qf, half_hashes, query_state,state)
      # this is unprepared error handler.
      %Error{reason: :unprepared} ->
        # first we use hardcoded cql statement of bundle query.
        cql = @bundle_cql
        FastGlobal.delete(cql)
        # fetch the bh,opts from the current query_state, because it might be a
        # response for paging request.
        %{opts: opts, bundle_hash: bh, current_index: ix, label: el, timestamp: ts}
          = query_state
        # we pass the bh,el,ts,ix,qf,opts as arguments to generate bundle query.
        {ok?, _, _query_state} = Helper.bundle_query(bh, el, ts, ix, qf, opts)
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
  def handle_cast({call, {:edge, qf}, buffer}, %{ref: ref} = state) when call in [:start, :stream] do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_all(call,buffer,query_state) do
      # this indicates some queries_states initited by
      # state[qf][:address] rows are might be ready
      # NOTE: it's impossible to have hint with :start/:stream handler,
      {%Compute{result: %{queries_states: queries_states}}, query_state} ->
        # update ref to add the new queries_states
        ref = length(queries_states) + ref
        # update state
        state = %{
          Enum.into(queries_states, state) |
          :ref => ref,
          qf => query_state
        }
        # return updated state
        {:noreply, state}
      {%Compute{result: {:error, reason}}, _} ->
        # we break.
        from = state[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
      %Ignore{state: query_state} ->
        state = %{state | qf => query_state}
        {:noreply, state}
    end
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:end, {:edge, qf}, buffer}, state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_full(buffer,query_state) do
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for address state[qf][:address] are ready.
      {%Compute{result: result}, %{has_more_pages: false}} when is_map(result) ->
        edge_case_has_more_pages_false(qf, result, state)
      # this indicates the queries_states for bundle queries which select transaction_hashes
      # for address state[qf][:address] are not completely ready, because
      # there still remaining address's rows should be retrieved using paging_state.
      {%Compute{result: result}, query_state} when is_map(result) ->
        edge_case_has_more_pages_true(qf, result, query_state, state)
      # this is {:error, _} handler
      {%Compute{result: {:error, reason}}, _} ->
        # we break.
        from = state[:from] # this the caller reference.
        # respond
        reply(from, {:error, reason})
        # stop worker
        {:stop, :normal, state}
    end
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({call, {:bundle, qf}, buffer}, state) when call in [:start, :stream] do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_all(call,buffer,query_state) do
      # this indicates some hashes for state[qf][:bundle_hash]
      # are might be ready
      {%Compute{result: hashes}, query_state} ->
        state = %{
          state |
          # add hashes to hashes_list
          :hashes => hashes ++ state[:hashes],
          qf => query_state
        }
        {:noreply, state}
      %Ignore{state: query_state} ->
        state = %{
          state |
          qf => query_state
        }
        {:noreply, state}
    end
  end

  @spec handle_cast(tuple, map) :: tuple
  def handle_cast({:end, {:bundle, qf}, buffer}, state) do
    # first we fetch the query state from the state using the qf key.
    query_state = Map.get(state, qf)
    # now we decode the buffer using the query_state.
    case Protocol.decode_end(buffer,query_state) do
      # this indicates the transaction_hashes for bundle_hash
      # state[qf][:bundle_hash] at current_index
      # state[qf][:current_index] are ready.
      {%Compute{result: hashes}, %{has_more_pages: false}} ->
        bundle_case_has_more_pages_false(qf, hashes, state)
      # this indicates the half_hashes are in half state.
      # which mean there still more rows(transactions_hashes)
      # have to be fetch with further queries requests.
      {%Compute{result: half_hashes}, query_state} ->
        bundle_case_has_more_pages_true(Helper,qf, half_hashes, query_state,state)
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
  @spec edge_case_has_more_pages_false(integer, map, map) :: tuple
  defp edge_case_has_more_pages_false(qf, %{queries_states: queries_states, hint: hint}, %{ref: ref, hints: hints_list} = state) do
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
        reply(from, {:ok, state[:hashes], hint ++ hints_list})
        # now we stop the worker.
        {:stop, :normal, state}
      _ ->
        # merge queries_states list with worker's state
        state = %{ Enum.into(queries_states, state) |
          ref: ref,
          hints: hint ++ hints_list
        }
        # return new state
        {:noreply, state}
    end
  end

  @doc false
  @spec edge_case_has_more_pages_true(integer, map, map, map) :: tuple
  defp edge_case_has_more_pages_true(
      qf,
      %{paging_state: p_state, opts: opts, address: address},
      %{queries_states: queries_states, hint: hint},
      %{ref: ref, hints: hints_list} = state) do
    # create a new edge query attached with paging_state
    # to fetch the remaining rows(address's rows)
    # Fetched opts, address to add them for the query request.
    # Adding paging_state to the opts.
    opts = Map.put(opts, :paging_state, p_state)
    # we pass the address, qf, opts as arguments
    # to generate edge query with paging_state.
    {ok?, _, query_state} = Helper.edge_query(address, qf, opts)
    # update ref to include the current queries_states count.
    ref = length(queries_states) + ref
    # update query_state in state
    state = %{Enum.into(queries_states, state) |
     qf => query_state,
     :ref => ref,
     :hints => hint ++ hints_list
    }
    # verfiy to proceed or break.
    ok?(ok?, state)
  end

end
