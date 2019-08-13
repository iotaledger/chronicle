defmodule Broker.Supervisor.Collector do

  @moduledoc """
    This the Collector supervisor which supervise the whole
    collector engine per topic.
  """

  use Supervisor
  alias Broker.Collector
  @spec start_link(list) :: tuple
  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: args[:name])
  end

  @spec init(list) :: tuple
  def init(args) do
    children = get_children_by_topic(args[:topic],args)
    Supervisor.init(children, strategy: :one_for_all)
  end

  defp get_children_by_topic(:tx_trytes,args) do
    # gen tx_feeder(s) children
    # first fetch nodes.
    nodes = args[:nodes]
    new_args = Keyword.drop(args, [:nodes, :name])
    tx_feeder_children =
      for {{host, port}, num} <- Enum.with_index(nodes) do
        # drop [nodes,name] as no longer needed and create new args
        args = new_args++[host: host, port: port, num: num]
        {Collector.TxFeeder, args}
      end
    transaction_partitions = args[:transaction_partitions]-1
    bundle_partitions = args[:bundle_partitions]-1
    children_1 =
      [
        {Collector.Distributor,new_args}
      ]
    children_2 =
      for num <- 0..transaction_partitions do
        {Collector.TransactionValidator, [num: num]++new_args}
      end
    children_3 =
      for num <- 0..transaction_partitions do
        {Collector.TxCollector, [num: num]++new_args}
      end
    children_4 =
      for num <- 0..bundle_partitions  do
        {Collector.TxBundleCollector, [num: num]++new_args}
      end
    children_5 =
      for num <- 0..bundle_partitions  do
        {Collector.BundleValidator, [num: num]++new_args}
      end
    tx_feeder_children++children_1++children_2++children_3++children_4++children_5
  end

  defp get_children_by_topic(:sn_trytes,args) do
    # gen sn_feeder(s) children
    # first fetch nodes.
    nodes = args[:nodes]
    new_args = Keyword.drop(args, [:nodes, :name])
    sn_feeder_children =
      for {{host, port}, num} <- Enum.with_index(nodes) do
        # drop [nodes,name] as no longer needed and create new args
        args = new_args++[host: host, port: port, num: num]
        {Collector.SnFeeder, args}
      end
    transaction_partitions = args[:transaction_partitions]-1
    bundle_partitions = args[:bundle_partitions]-1
    children_1 =
      [
        {Collector.Distributor,new_args}
      ]
    children_2 =
      for num <- 0..transaction_partitions do
        {Collector.TransactionValidator, [num: num]++new_args}
      end
    children_3 =
      for num <- 0..transaction_partitions do
        {Collector.SnCollector, [num: num]++new_args}
      end
    children_4 =
      for num <- 0..bundle_partitions  do
        {Collector.SnBundleCollector, [num: num]++new_args}
      end
    children_5 =
      for num <- 0..bundle_partitions  do
        {Collector.BundleValidator, [num: num]++new_args}
      end
    sn_feeder_children++children_1++children_2++children_3++children_4++children_5
  end

end
