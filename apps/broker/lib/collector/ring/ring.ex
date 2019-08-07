defmodule Broker.Collector.Ring do

  @tx_partitions Application.get_env(:broker, :__TRANSACTION_PARTITIONS_PER_TOPIC__)
  @bundle_partitions Application.get_env(:broker, :__BUNDLE_PARTITIONS_PER_TOPIC__)

  # public functions ####### start ##
  @spec tx_collector?(binary) :: atom
  def tx_collector?(hash) do
    :erlang.phash2(hash, @tx_partitions)
    |> lookup_tx_collector()
  end
  @spec sn_collector?(binary) :: atom
  def sn_collector?(hash) do
    :erlang.phash2(hash, @tx_partitions)
    |> lookup_tx_collector()
  end
  @spec tx_bundle_collector?(binary) :: atom
  def tx_bundle_collector?(hash) do
    :erlang.phash2(hash, @bundle_partitions)
    |> lookup_tx_bundle_collector()
  end
  @spec sn_bundle_collector?(binary) :: atom
  def sn_bundle_collector?(hash) do
    :erlang.phash2(hash, @bundle_partitions)
    |> lookup_sn_bundle_collector()
  end
  # public functions ####### end ##

  # private functions ####### start ##
  for n <- 0..@tx_partitions-1 do
    tx_collector_pid_name = :"tc#{n}"
    @spec lookup_tx_collector(integer) :: atom
    defp lookup_tx_collector(unquote(n)) do
      unquote(tx_collector_pid_name)
    end
  end
  for n <- 0..@tx_partitions-1 do
    sn_collector_pid_name = :"sc#{n}"
    @spec lookup_sn_collector(integer) :: atom
    defp lookup_sn_collector(unquote(n)) do
      unquote(sn_collector_pid_name)
    end
  end
  for n <- 0..@bundle_partitions-1 do
    tx_bundle_collector_pid_name = :"tbc#{n}"
    @spec lookup_tx_bundle_collector(integer) :: atom
    defp lookup_tx_bundle_collector(unquote(n)) do
      unquote(tx_bundle_collector_pid_name)
    end
  end
  for n <- 0..@bundle_partitions-1 do
    sn_bundle_collector_pid_name = :"sbc#{n}"
    @spec lookup_sn_bundle_collector(integer) :: atom
    defp lookup_sn_bundle_collector(unquote(n)) do
      unquote(sn_bundle_collector_pid_name)
    end
  end
  # private functions ####### end ##

end
