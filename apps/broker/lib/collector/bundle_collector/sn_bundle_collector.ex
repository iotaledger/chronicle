defmodule Broker.Collector.SnBundleCollector do
  @moduledoc """

  Documentation for Broker.Collector.SnBundleCollector.
  This lightweight processor is responsible to collect bundles
  it receives the tx-head-object(s) from random tx-collector(s)
  then it starts asking targeted sn-collector for a given tx? by trunk.

  """

  use GenStage
  require Logger
  require Broker.Collector.Ring
  alias Broker.Collector.Ring

  @bundle_tll Application.get_env(:broker, :__BUNDLE_TTL__) || 30000

  def start_link(args) do
    name = :"sbc#{args[:num]}"
    GenStage.start_link(__MODULE__, %{name: name}, name: name)
  end

  def init(state) do
    Process.put(:name, state[:name])
    {:producer, state}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, tuple) :: tuple
  def handle_subscribe(:consumer, _, _from, state) do
    Logger.info("SnBundleCollector: #{Process.get(:name)} got subscribed_to BundleValidator")
    {:automatic, state}
  end

  @spec handle_demand(integer,tuple) :: tuple
  def handle_demand(_, state) do
    {:noreply, [], state}
  end

  def handle_info({:active?, current_index, hash},state) do
    # fetch tx_object or nil from the state by hash
    state =
      case state[hash] do
        # we check with the head
        [%{current_index: ^current_index} |_] ->
          # drop bundle because its not active.
          Logger.warn("Wasn't able to collect sn_bundle: #{hash}")
          Map.delete(state, hash)
        _nil_or_tx_object_with_updated_current_index ->
          # return state
          state
      end
    {:noreply,[], state}
  end

  # handle new flow of head_transactions(new bundles)
  def handle_cast({:new, tx_object}, %{name: name} = state) do
      case tx_object do
        %{last_index: 0} ->
          # process it now as it's a complete bundle.
          # dispatch it to bundleValidator
          # create bundle with length = 1
          bundle = [tx_object]
          # return untouched state
          {:noreply,[bundle], state}
        %{hash: hash, trunk: trunk} ->
          # put it in state
          state = Map.put_new(state, hash, [tx_object])
          # send after to free non-active bundle from state,
          # we tell if the bundle is not active by checking
          # the recent transaction index is still the same.
          Process.send_after(self(), {:active?, 0, hash}, @bundle_tll)
          # we should request a tx_collector by using the trunk
          sn_collector_pid_name = Ring.sn_collector?(trunk)
          GenServer.cast(sn_collector_pid_name,{:tx?, trunk, hash, name})
          # return state with no events, as no bundle was collected.
          {:noreply,[], state}
      end
  end

  #
  def handle_cast({ref_id, tx_object}, %{name: name} = state) do
    # ref_id is the head hash, first we fetch the ref_id state
      case Map.get(state, ref_id) do
        bundle when is_list(bundle) ->
          new_ref_id_state = [tx_object | bundle]
          # fetch the required info from tx_object
          %{trunk: trunk, current_index: cx, last_index: lx, hash: hash} = tx_object
          if cx == lx do
            # the bundle ready for processing/verifying, drop bundle from state
            {:noreply,[new_ref_id_state], Map.delete(state, ref_id)}
          else
            # the bundle not ready yet.
            # send_after to delete the bundle if it was not active.
            Process.send_after(self(), {:active?,cx,hash}, @bundle_tll)
            # we should request a tx_collector by using the trunk
            sn_collector_pid_name = Ring.sn_collector?(trunk)
            GenServer.cast(sn_collector_pid_name,{:tx?, trunk, ref_id, name})
            # return updated state
            state = Map.put(state, ref_id, new_ref_id_state)
            {:noreply, [], state}
          end
        nil ->
          # this means the bundle has already being deleted before receiving the request response.
          # NOTE: this might only happen due to rare race condition, and that's totally fine
          # as long we return the state
          {:noreply, [], state}
      end
  end

  def child_spec(args) do
    %{
      id: :"sbc#{args[:num]}",
      start: {__MODULE__, :start_link, [args]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

end
