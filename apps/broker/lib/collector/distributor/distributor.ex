defmodule Broker.Collector.Distributor do
  @moduledoc """

  Documentation for Broker.Collector.Distributor.
  This lightweight processor will receive all the
  flow of transactions from tx_feeder(s) or sn_feeder(s) and it will
  insert them to FIFO queue and will receive ask
  requests from tx_validator(s) or sn_feeder(s).
  """
  alias Broker.Collector.Distributor.Helper

  @interval Application.get_env(:broker, :__INTERVAL__) || 1000

  require Logger
  use GenStage

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    name = name_by_topic(args)
    GenStage.start_link(__MODULE__, [name: name]++args, name: name)
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    Process.put(:name, args[:name])
    state = %{queue: :queue.new(), demand: 0}
    {:producer, state}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, tuple) :: tuple
  def handle_subscribe(:consumer, _, _from, state) do
    Logger.info("Distributor: #{Process.get(:name)} got subscribed_to Validator")
    {:automatic, state}
  end

  @spec handle_demand(integer,tuple) :: tuple
  def handle_demand(demand,%{queue: queue,demand: pending_demand} = state) do
    total_demand = pending_demand+demand
    {events, queue} = Helper.take_unique(queue, total_demand)
    pending_demand = total_demand-length(events)
    if pending_demand != 0 do
      Process.send_after(self(), :demand, @interval)
    end
    {:noreply, events, %{state | demand: pending_demand, queue: queue}}
  end

  def handle_info(:demand, %{demand: 0} = state) do
    {:noreply, [], state}
  end

  def handle_info(:demand, %{queue: queue,demand: pending_demand} = state) do
    {events, queue} = Helper.take_unique(queue, pending_demand)
    pending_demand = pending_demand-length(events)
    if pending_demand != 0 do
      Process.send_after(self(), :demand, @interval)
    end
    {:noreply, events, %{state | demand: pending_demand, queue: queue}}
  end

  @doc """
    Here the Distributor receives transactions flow from feeder(s)
    and insert them into the queue.
  """
  @spec handle_cast(list, map) :: tuple
  def handle_cast({:event, event}, %{queue: queue} = state) do
    queue = :queue.in(event, queue)
    {:noreply, [], %{state | queue: queue}}
  end

  defp name_by_topic(args) do
    case args[:topic] do
      :tx_trytes ->
        :tx_distributor
      :sn_trytes ->
        :sn_distributor
    end
  end

  def child_spec(args) do
    %{
      id: name_by_topic(args),
      start: {__MODULE__, :start_link, [args]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end
end
