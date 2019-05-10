defmodule Core.Supervisor.Cluster do

  @moduledoc """
    This the cluster supervisor which supervise the DymanicCluster
    supervisor and the Cluster Manager.
  """

  use Supervisor

  @spec start_link(list) :: tuple
  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: :"#{args[:otp_app]}_#{args[:data_center]}")
  end
  
  @spec init(list) :: tuple
  def init(args) do
    children = [
    {Core.Supervisor.DynamicCluster, args},
      {Core.Supervisor.Cluster.Manager, args}
    ]
    Supervisor.init(children, strategy: :one_for_all)
  end


end
