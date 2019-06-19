defmodule ExtendedApi.Worker.Helper do
  @moduledoc """
    This module hold all the common functions accross workers.
  """

  @doc """
    # this check if the query has been sent
    # to the shard's stage(reporter), and its result
    # determin whether to stop GetTrytes worker or proceed.
  """
  @spec ok?(atom, map) :: tuple
  def ok?(ok?, state) do
    case ok? do
      :ok ->
        {:noreply, state}
      _ ->
        # we break and respond.
        from = state[:from] # this the caller reference.
        reply(from, {:error, {:dead_shard_stage, ok?} } )
        {:stop, :normal, state}
    end
  end

  # this is used internally to reply msg to the caller.
  @spec reply(tuple, list | tuple) :: :ok
  def reply(from, msg) do
    GenServer.reply(from, msg)
  end


  @doc false
  @spec bundle_case_has_more_pages_false(integer,list, map) :: tuple
  def bundle_case_has_more_pages_false(qf, hashes, %{ref: ref, hashes: hashes_list} = state) do
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
        reply(from, {:ok, hashes ++ hashes_list, state[:hints]})
        # now we stop the worker.
        {:stop, :normal, state}
      _ ->
        # this indicates it's not the last response.
        # (mean there are other queries under progress.)
        # no longer need query_state for qf in state.
        state = %{ Map.delete(state, qf) |
          # we preappend hashes with hashes_list.
          hashes: hashes ++ hashes_list,
          ref: ref
        }
        # return updated state.
        {:noreply, state}
    end
  end

  @doc false
  @spec bundle_case_has_more_pages_true(atom,integer, list, map, map) :: tuple
  def bundle_case_has_more_pages_true(helper_mod,
      qf,
      half_hashes,
      %{opts: opts, bundle_hash: bh, current_index: ix, label: el, timestamp: ts, paging_state: p_state},
      %{hashes: hashes_list} = state) do
    # pattern matching on opts,bh,ix,el,ts from the query_state,
    # as we needed(opts, bh, ix,el, ts) to issue new bundle_query.
    # add paging_state to opts
    opts = Map.put(opts, :paging_state, p_state)
    # we pass bh,el,ts,ix,opts as arguments
    # to generate bundle query with paging_state.
    {ok?, _, query_state} = helper_mod.bundle_query(bh, el, ts, ix, opts)
    # we preappend half_hashes with hashes_list.
    # update state
    state = %{state |
      :hashes => half_hashes  ++ hashes_list,
      qf => query_state
      }
    # verfiy to proceed or break.
    ok?(ok?,  state)
  end

end
