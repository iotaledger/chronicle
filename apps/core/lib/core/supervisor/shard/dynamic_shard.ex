defmodule Core.Supervisor.DynamicShard do

  @moduledoc """
    This the DymanicShard supervisor which supervise the Conn supervisors.
  """

  use DynamicSupervisor

  @spec start_link(list) :: tuple
  def start_link(args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: :"#{args[:ring_key]}_d#{args[:shard]}")
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

end
