defmodule Broker.Collector.TxCollector do
  @moduledoc """

  Documentation for Broker.Collector.TxCollector.
  This lightweight processor(consumer) is responsible to handle dispatched
  flow of transactions from Feeder(s) producer.

  """
  use GenStage
  require Logger
  require Broker.Collector.Ring
  alias Broker.Collector.Ring
  alias Broker.Collector.TxCollector.Helper

  @tx_ttl Application.get_env(:broker, :tx_ttl) || 10000
  @bundle_ttl Application.get_env(:broker, :bundle_ttl) || 10000
  @feeders Application.get_env(:broker, :feeders) || 2 # # TODO: change it to 2 once sn topic is ready

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: args[:name])
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    p = args[:partition]
    subscribe_to = for n <- 1..@feeders do
      {:"t#{n}", partition: p}
    end
    {:consumer, %{subscription: nil}, subscribe_to: subscribe_to}
  end

  def handle_subscribe(:producer, _options, from, state) do
    new_state = %{state | subscription: from}
    Logger.info("TxCollector: #{Process.get(:name)} got subscribed_to Feeder")
    {:automatic, new_state}
  end

  @doc """
    here we should handle the the transaction flow,
    - verfiy if hash is legit.
    first we check if the tx by hash if is already in the waiting list(map),
    if so we send it to the corsponding bundle_collectors and then delete it from
    the waiting map.
    - otherwise we store it in the map, and send_self to delete it after interval.
  """
  @spec handle_events(list, tuple, map) :: tuple
  def handle_events(events, _from, state) do
    # process events and return updated state.
    state = process_events(events, state)
    {:noreply, [], state}
  end

  @doc """
    This receive request for tx from bundle_collector,
    first it checks whether the tx is already in the map,
    if so it sends the tx to pid_name, and delete it from the map.
    else it appends the pid_name to the waiting list,
    and send_self to delete the pid_name from the waiting map.
  """
  def handle_cast({:tx?, hash, ref_id, pid_name}, state) do
    # process_request
    process_request(hash, ref_id, pid_name, state)
    {:noreply, [], state}
  end

  # delete pid_name from b_collectors_list or delete the whole hash from state
  # if counter = 1
  # handler to remove request from state
  def handle_info({:rm, hash}, state) do
    state =
      case Map.fetch!(state, hash) do
        {1,_} ->
          Map.delete(state, hash)
        {counter, [_| b_collectors_list]} ->
          Map.put(state, hash, {counter-1, b_collectors_list})
        _ ->
          state
      end
    {:noreply, [], state}
  end

  def handle_info({:free, hash}, state) do
    # detele the hash from the state
    state = Map.delete(state, hash)
    {:noreply, [], state}
  end

  # start of private functions related to processing events
  defp process_events([{hash, trytes}|tail], state) do
    state = process_event(hash, trytes, state)
    process_events(tail, state)
  end

  defp process_events([], state) do
    state
  end

  defp process_event(hash, trytes, state) do
    # verify hash
    if Helper.verify_hash(hash, trytes) do
      # this mean a valid transaction
      # we create a tx-object(struct)
      tx_object = Helper.create_tx_object(hash, trytes)
      # we process the tx_object.
      process_tx_object(hash,tx_object,state)
    else
      # invalid tx so we ignore it.
      # return state.
      state
    end
  end

  # current_index: 0, is_head = true. so no processing is needed.
  defp process_tx_object(hash, %{current_index: 0} = tx_object, state) do
    # target a bundle_collector by tx_hash
    pid_name = Ring.bundle_collector?(hash)
    # send this to a bundle_collector
    GenStage.cast(pid_name, {:new, tx_object})
    # return state
    state
  end

  defp process_tx_object(hash, tx_object, state) do
    # this means the tx is not a head.
    # put tx_object in the state only if it's not in the waiting list.
    # check whether already requested by bundle_collector(s) in the map.
    case Map.put_new(state, hash, tx_object) do
      %{^hash => {_, b_collectors_list}} ->
        # send the tx_object to b_collectors_list and return updated state
        for {ref_id, pid_name} <- b_collectors_list do
          GenStage.cast(pid_name, {ref_id, tx_object})
        end
        # return updated state
        Map.delete(state, hash)
      new_state ->
        # send_self to delete it after interval.
        Process.send_after(self(), {:free, hash}, @tx_ttl)
        new_state
    end
  end
  # end of private functions related to processing events

  # start of private functions related to processing :tx? request

  defp process_request(hash, ref_id, pid_name, state) do
    # fetch the tx-object/waiters(if any) from state
    case Map.fetch!(state, hash) do
      tx_object when is_map(tx_object) ->
        # send tx_object to pid_name with ref_id
        GenStage.cast(pid_name, {ref_id, tx_object})
        # delete it from state
        Map.delete(state, hash)
      {counter, b_collectors_list} ->
        # this indicates we already have a prev requester who asked for the same hash.
        # - increase the counter and append {ref_id, pid_name} to the end.
        b_collectors = {counter+1, b_collectors_list++[{ref_id, pid_name}]}
        # send_self to delete the b_collector from b_collectors_list or
        # delete the whole request if counter=1. note: using @bundle_ttl
        Process.send_after(self(), {:rm, hash}, @bundle_ttl)
        # return updated state
        Map.put(state, hash, b_collectors)
      nil ->
        # create a b_collector tuple.
        b_collectors = {1, [{ref_id,pid_name}]}
        # send_self to delete the request @bundle_ttl
        Process.send_after(self(), {:rm, hash}, @bundle_ttl)
        # add it to the state
        Map.put(state, hash, b_collectors)
    end
  end

end
