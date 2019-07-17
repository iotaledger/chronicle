defmodule Broker.Collector.Feeder do
  @moduledoc """

  Documentation for Broker.Collector.Feeder.
  This lightweight processor is responsible to handle random
  flow of transactions, mostly through ZMQ topic.

  """
  use GenStage
  require Logger
  alias GenStage.PartitionDispatcher

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: args[:name])
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    # put name in process dict state
    Process.put(:name, args[:name])
    # fetch partitions num
    p = args[:partitions]
    # this function handle the event(tx)
    # NOTE: dispatching should be applied only on the transaction hash.
    function = fn event -> {event, :erlang.phash2(event[:hash], p)} end
    {:producer, %{}, dispatcher: {PartitionDispatcher, partitions: 0..p-1}, hash: function}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:consumer, _, _from, state) do
    Logger.info("Feeder: #{Process.get(:name)} got subscribed_to TxCollector")
    {:automatic, state}
  end

  @spec handle_demand(integer, map) :: tuple
  def handle_demand(demand, state) when demand > 0 do
    {:noreply, [], state}
  end

end
