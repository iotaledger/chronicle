defmodule Broker.Collector.SnFeeder do
  @moduledoc """

  Documentation for Broker.Collector.SnFeeder.
  This lightweight processor is responsible to handle random or ordered
  flow of confirmed transactions, through ZMQ 'sn_trytes' topic
  with the following format: <<trytes, hash, milestone_index>>

  """
  use GenStage
  require Logger
  alias Broker.Collector.Feeder.Helper

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    name =  :"sf#{args[:num]}"
    GenStage.start_link(__MODULE__, [{:name, name}| args], name: args[:name])
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    # put name in process dict state
    Process.put(:name, args[:name])
    # fetch/put topic
    Process.put(:topic, :sn_trytes)
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

  def handle_info(:sn_trytes, socket) do
    events =
      case Helper.recv(socket) do
        {:ok, <<"sn_trytes ", tx_trytes::2673-bytes,_,hash::81-bytes,_, snapshot_index::binary>>} ->
          # loop
          send(self(), :sn_trytes)
          # create events
          [{hash, tx_trytes, String.to_integer(snapshot_index)}]
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
