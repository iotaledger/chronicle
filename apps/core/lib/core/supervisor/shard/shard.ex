defmodule Core.Supervisor.Shard do

  @moduledoc """
    This the Shard supervisor which supervise the DymanicShard
    supervisor and the Shard Manager.
  """

  use Supervisor

  @spec start_link(list) :: tuple
  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: :"#{args[:ring_key]}_#{args[:shard]}")
  end

  @spec init(list) :: tuple
  def init(args) do
    children = [
    {Core.Supervisor.DynamicShard, args},
      {Core.Supervisor.Shard.Manager, args}
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end


end
