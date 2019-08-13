defmodule Broker.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false
  require Logger
  use Application

  def start(_type, _args) do
    Logger.info("Starting broker app")
    # get otp_app name which is by :broker
    otp_app = Application.get_application(__MODULE__)
    # get the topics
    topics = Application.get_env(otp_app, :__TOPICS__)
    # get the transaction_validators_per_topic
    transaction_partitions = Application.get_env(otp_app, :__TRANSACTION_PARTITIONS_PER_TOPIC__)
    # get the bundle_validators_per_topic
    bundle_partitions = Application.get_env(otp_app, :__BUNDLE_PARTITIONS_PER_TOPIC__)
    # create extra_args
    extra_args = [transaction_partitions: transaction_partitions, bundle_partitions: bundle_partitions]
    # gen_children
    children =
      gen_children(topics[:tx_trytes],:tx_trytes,extra_args) ++ gen_children(topics[:sn_trytes],:sn_trytes,extra_args)
    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Broker.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp gen_children(nodes,topic,extra_args) when is_list(nodes) and nodes != [] and not is_nil(nodes) do
    name = :"#{topic}_supervisor"
    [
      %{
        id: name,
        start: {Broker.Supervisor.Collector, :start_link, [[nodes: nodes, topic: topic, name: name]++extra_args]}
      }
    ]
  end
  # return empty children
  defp gen_children(_,_,_) do
    []
  end
end
