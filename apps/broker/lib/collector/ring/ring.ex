defmodule Broker.Collector.Ring do

  @partitions Application.get_env(:broker, :partitions) || 20

  for n <- 0..@partitions-1 do
    tx_collector_pid_name = :"t#{n}"
    bundle_collector_pid_name = :"b#{n}"
    @spec lookup_tx_collector(integer) :: atom
    def lookup_tx_collector(unquote(n)) do
      unquote(tx_collector_pid_name)
    end
    @spec lookup_bundle_collector(integer) :: atom
    def lookup_bundle_collector(unquote(n)) do
      unquote(bundle_collector_pid_name)
    end
    @spec tx_collector?(binary) :: atom
    def tx_collector?(hash) do
      :erlang.phash2(hash, @partitions)
      |> lookup_tx_collector()
    end
    @spec bundle_collector?(binary) :: atom
    def bundle_collector?(hash) do
      :erlang.phash2(hash, @partitions)
      |> lookup_bundle_collector()
    end
  end

end
