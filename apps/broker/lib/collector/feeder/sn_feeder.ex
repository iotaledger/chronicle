defmodule Broker.Collector.SnFeeder do
  @moduledoc """

  Documentation for Broker.Collector.SnFeeder.
  This lightweight processor is responsible to handle random
  flow of transactions, through ZMQ 'sn_trytes' topic with
  the following format: <<tx_trytes, hash, snapshot_index>>

  """
  use GenServer
  require Logger
  alias Broker.Collector.Feeder.Helper

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    name =  :"sf#{args[:num]}"
    GenServer.start_link(__MODULE__, [{:name, name}| args], name: name)
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
    {:ok, nil}
  end

  def handle_info(:sn_trytes, socket) do
    case Helper.recv(socket) do
      {:ok, <<"sn_trytes ", tx_trytes::2673-bytes,_,hash::81-bytes,_,snapshot_index::binary>>} ->
        # send event to distributor
        GenServer.cast(:sn_distributor,{:event, {hash, tx_trytes,String.to_integer(snapshot_index)}})
        # loop to receive the next sn_trytes
        send(self(), :sn_trytes)
      {:error, _reason} ->
        # handle error, moslty establishing a new socket.
        send(self(), :setup)
    end
    {:noreply, socket}
  end

  def handle_info(:setup, socket_or_nil) do
    topic = Process.get(:topic)
    case Helper.setup_feed_source(topic,
      Process.get(:host),Process.get(:port)) do
        {:ok, socket} ->
          send(self(), topic)
          {:noreply, socket}
        {:error, reason} ->
          send(self(), :setup)
          {:noreply, socket_or_nil}
    end
  end

  def child_spec(args) do
    %{
      id: :"sf#{args[:num]}",
      start: {__MODULE__, :start_link, [args]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

end
