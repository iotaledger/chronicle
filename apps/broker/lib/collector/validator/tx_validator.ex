defmodule Broker.Collector.TxValidator do

  @max_demand Application.get_env(:broker, :max_demand) || 64
  @min_demand Application.get_env(:broker, :min_demand) || 0

  require Logger

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    GenStage.start_link(__MODULE__, [], name: args[:name])
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    partitions = args[:partitions]
    dispatch_fn = fn(event) -> {event, :erlang.phash2(elem(0, event), partitions)} end
    opts = [
      # this option make TxValidator act as partitionDispatcher to TxCollectors
      dispatcher: {GenStage.PartitionDispatcher, partitions: partitions,
        hash: dispatch_fn},
      # this option to subscribe to TxValidator producer(distributor)
      subscribe_to: [{args[:distributor], max_demand: @max_demand, min_demand: @min_demand}]
    ]
    {:producer_consumer, %{producer: nil}, opts}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:producer, _, from, state) do
    # we add producer(distributor from_reference to be able to send ask requests)
    state = %{producer: from}
    Logger.info("TxValidator: #{Process.get(:name)} got subscribed_to Distributor")
    {:manual, state}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:consumer, _, _from, state) do
    Logger.info("TxValidator: #{Process.get(:name)} got subscribed_to TxCollector")
    # we keep it automatic to dispatch events on the fly
    {:automatic, state}
  end

  @doc """
    Handle events from producer(distributor)
    - validate the events(txs)
    - ask for max_demand
    - send as events to tx_collector(s)
  """
  @spec handle_events(list, tuple, map) :: tuple
  def handle_events(events, _from, state) do

    {:noreply, [], state}
  end
end
