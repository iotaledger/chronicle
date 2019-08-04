defmodule Broker.Collector.Distributor do
  @moduledoc """

  Documentation for Broker.Collector.Distributor.
  This lightweight processor will receive all the
  flow of transactions from sn_feeder(s) and it will
  insert them to FIFO queue and will receive ask
  requests from sn_validator(s).

  # NOTE: this is a :producer_consumer
  """
  @max_demand Application.get_env(:broker, :__MAX_DEMAND__) || 64
  @sn_feeders_num length(Application.get_env(:broker, :__TOPICS__)[:sn_trytes])

  require Logger
  use GenStage

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: :sn_distributor)
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    Process.put(:name, :tx_distributor)
    opts = [
      subscribe_to:
        for n <- 1..@sn_feeders_num do
          :"sf#{n}"
        end
    ]
    {:producer_consumer, :queue.new(), opts}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, tuple) :: tuple
  def handle_subscribe(:consumer, _, _from, queue) do
    Logger.info("SnDistributor: #{Process.get(:name)} got subscribed_to SnValidator")
    {:manual, queue}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:producer, _, _from, queue) do
    Logger.info("SnDistributor: #{Process.get(:name)} got subscribed_to SnFeeder")
    {:automatic, queue}
  end

  @spec handle_demand(integer, tuple) :: tuple
  def handle_demand(demand, queue) when demand > 0 do
    {events, queue} = take_unique(queue, @max_demand)
    {:noreply, events, queue}
  end

  @doc """
    Here the Distributor receives transactions flow from feeder(s)
    and insert them into the queue.
  """
  @spec handle_events(list, tuple, map) :: tuple
  def handle_events(events, _from, queue) do
    queue = add_events_to_queue(events, queue)
    {:noreply, [], queue}
  end

  # add events into the queue
  defp add_events_to_queue([event | events], queue) do
    add_events_to_queue(events,:queue.in(queue, event))
  end

  defp add_events_to_queue([], queue) do
    queue
  end

  # take unique elements from queue
  defp take_unique(q, amount), do: do_take(q, amount, [], amount)
  defp do_take(q, n, acc, n_acc) when n > 0 do
    case :queue.out(q) do
      {{:value, e}, rest} ->
        do_take(rest, n - 1, [e | acc],n_acc)
      {:empty, q} ->
        {Enum.uniq(acc), q}
    end
  end
  defp do_take(q, _, acc, n_acc) do
    acc = Enum.uniq(acc)
    case length(acc) do
      ^n_acc ->
       {acc, q}
      acc_length ->
        do_take(q, n_acc-acc_length, acc, n_acc)
    end
  end

end
