defmodule Broker.Collector.SnFeeder do
  @moduledoc """

  Documentation for Broker.Collector.SnFeeder.
  This lightweight processor is responsible to handle random
  flow of confirmed transactions, through ZMQ 'sn_trytes' topic
  with the following format: <<tx_trytes, hash, milestone_index>>

  """
  use GenStage
  require Logger
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
    # setup
    send(self(), :setup)
    {:producer, nil}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:consumer, _, _from, state) do
    Logger.info("SnFeeder: #{Process.get(:name)} got subscribed_to SnDistributor")
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
          [{hash, tx_trytes, nil}]
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
