defmodule Broker.Collector.BundleCollector do
  @moduledoc """

  Documentation for Broker.Collector.BundleCollector.
  This lightweight processor is responsible to collect bundles
  it receives the tx-head-object(s) from random tx-collector(s)
  then it starts asking targeted tx-collector for a given tx? by trunk.

  """
  # NOTE: work in progress

  use GenServer
  @bundle_tll Application.get_env(:broker, :bundle_ttl) || 10000

  def start_link(args) do
    GenServer.start_link(__MODULE__, %{}, name: args[:name])
  end

  def init(state) do
    {:ok, state}
  end

  # handle new flow of head_transactions(new bundles)
  def handle_cast({:new, tx_object}, state) do
    state =
      case tx_object do
        %{last_index: 0} ->
          # process it now as it's a complete bundle.
          # we have to verify the bundle

          # send it to inserter if it's a valid bundle
          # return untouched state
          state
        %{hash: hash} ->
          # put it in state
          state = Map.put_new(state, hash, [tx_object])
          # send after to free non-active bundle from state,
          # we tell if the bundle is not active by checking
          # the recent transaction index is still the same.
          Process.send_after(self(), {:free, 0, hash}, @bundle_tll)
          # return state
          state
      end
    {:noreply, state}
  end

  #
  def handle_cast({ref_id, tx_object}, state) do
    # ref_id is the head hash, first we fetch the ref_id state
    case Map.fetch!(state, ref_id) do
      list when is_list(list) ->
        new_ref_id_state = [tx_object | list]
        Map.put(state, ref_id, new_ref_id_state)
      nil ->
        #
    end

    {:noreply, state}
  end

end
