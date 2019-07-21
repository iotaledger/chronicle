defmodule Broker.Collector.Feeder do
  @moduledoc """

  Documentation for Broker.Collector.Feeder.
  This lightweight processor is responsible to handle random
  flow of transactions, mostly through ZMQ topic.

  """
  use GenStage
  require Logger
  alias GenStage.PartitionDispatcher
  alias Broker.Collector.Feeder.Helper

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: args[:name])
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    # put name in process dict state
    Process.put(:name, args[:name])
    # fetch/put topic
    Process.put(:topic, args[:topic])
    # fetch/put host
    Process.put(:host, args[:host])
    # fetch/put port
    Process.put(:port, args[:port])
    # fetch partitions num
    p = args[:partitions]
    # this function handle the event(tx)
    # NOTE: dispatching should be applied only on the transaction hash.
    function = fn event -> {event, :erlang.phash2(elem(event,0), p)} end
    {:producer, nil, dispatcher: {PartitionDispatcher, partitions: 0..p-1}, hash: function}
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

  def handle_info(:tx_trytes, socket) do
    events =
      case Helper.recv(socket) do
        {:ok, <<"tx_trytes ", tx_trytes::2755-bytes,_,hash::81-bytes>>} ->
          # loop
          send(self(), :tx_trytes)
          # create events
          [{hash, tx_trytes}]
        {:error, _reason} ->
          # handle error, moslty establishing a new socket.
          send(self(), :setup)
          # return empty events
          []
      end
    {:noreply, events, socket}
  end

  def handle_info(:setup, _socket_or_nil) do
    topic = Process.get(:topic)
    case Helper.setup_feed_source(topic,
      Process.get(:host),Process.get(:port)) do
        {:ok, socket} ->
          send(self(), topic)
          {:noreply, [], socket}
        {:error, reason} ->
          send(self(), :setup)
    end
  end

end
