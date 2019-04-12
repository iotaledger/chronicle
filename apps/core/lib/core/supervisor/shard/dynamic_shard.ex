defmodule Core.Supervisor.DynamicShard do

  use DynamicSupervisor

  def start_link(args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: :"#{args[:ring_key]}_d#{args[:shard]}")
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end




end
