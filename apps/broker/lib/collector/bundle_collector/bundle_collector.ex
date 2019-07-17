defmodule Broker.Collector.BundleCollector do
  @moduledoc """

  Documentation for Broker.Collector.BundleCollector.
  This lightweight processor is responsible to collect bundles
  it receives the tx-head-object(s) from random tx-collector(s)
  then it starts asking targeted tx-collector for a given tx? by trunk.

  """
  use GenStage
  alias GenStage.PartitionDispatcher
  
end
