defmodule Core.Supervisor.Shard do

  use Supervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: :"#{args[:ring_key]}_#{args[:shard]}")
  end

  def init(args) do
    children = [
    {Core.Supervisor.DynamicShard, args},
      {Core.Supervisor.Shard.Manager, args}
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end


end
