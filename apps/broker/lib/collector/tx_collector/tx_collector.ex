defmodule Broker.Collector.TxCollector do
  @moduledoc """

  Documentation for Broker.Collector.TxCollector.
  This lightweight processor(consumer) is responsible to handle dispatched
  flow of transactions from Feeder(s) producer.

  """
  use GenStage
  require Logger

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: args[:name])
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    {:consumer, %{subscription: nil}}
  end

  def handle_subscribe(:producer, _options, from, state) do
    new_state = %{state | subscription: from}
    {:automatic, new_state}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:producer, _, _, state) do
    Logger.info("TxCollector: #{Process.get(:name)} got subscribed_to Feeder")
    {:automatic, state}
  end

  @doc """
    here we should handle the the transaction flow,
    first we check if the tx by hash if is already in the waiting list(map),
    if so we send it to the corsponding bundle_collectors and then delete it from
    the waiting map.
    - otherwise we store it in the map, and send_self to delete it after interval.
  """
  @spec handle_events(list, tuple, map) :: tuple
  def handle_events(events, _from, state) do
    #
    {:noreply, [], state}
  end

  @doc """
    This receive request for tx from bundle_collector(s),
    first it checks whether the tx is already in the map,
    if so it sends the tx to pid_name,
    else it adds the pid_name to the waiting map,
    and send_self to delete the pid_name from the waiting map.
  """
  def handle_cast({:tx?, hash, ref_id, pid_name}, state) do
    #
    {:noreply, [], state}
  end

end
