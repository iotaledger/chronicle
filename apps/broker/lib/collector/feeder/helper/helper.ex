defmodule Broker.Collector.Feeder.Helper do

  @moduledoc """
    This module hold the helper ZMQ functions.
  """
  @spec setup_feed_source(binary, list, integer) :: tuple
  def setup_feed_source(topic,host,port) when is_binary(topic) and is_list(host) and is_integer(port) do
    # first we create a zmq_socket and subscribe to that topic
    case create_zmq_socket_and_subscribe(topic) do
      {:ok, socket} ->
        connect_to_socket(socket, host, port)
      err? ->
        err?
    end
  end

  @spec create_zmq_socket_and_subscribe(atom) :: pid
  defp create_zmq_socket_and_subscribe(:tx_trytes) do
    create_zmq_socket_and_subscribe("tx_trytes")
  end

  # create socket pid and subscribe to a topic.
  # it returns :ok.
  @spec create_zmq_socket_and_subscribe(binary) :: pid
  defp create_zmq_socket_and_subscribe(topic) when is_binary(topic) do
    case :chumak.socket(:sub) do
      {:ok, socket} = result ->
        # sub to topic
        :chumak.subscribe(socket,topic)
        # return result
        result
      err? -> err?
    end
  end

  @doc """
    connect to socket and returns {:ok, _bindpid}
    or {:error, reason}.
  """
  @spec connect_to_socket(pid, list, integer) :: tuple
  def connect_to_socket(socket, host, port) when is_pid(socket) and is_list(host) and is_integer(port) do
    :chumak.connect(socket,:tcp, host, port)
  end

  @doc """
    blocking-Receive binary data from a socket(pid),
    This function should be called in event-driven loop.
  """
  @spec recv(pid) :: {:ok, binary} | {:error, atom}
  def recv(socket) do
    :chumak.recv(socket)
  end

end
