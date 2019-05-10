defmodule Core.Supervisor.DynamicCluster do

  @moduledoc """
    This the DymanicCluster supervisor which supervise the Node supervisors.
  """

  use DynamicSupervisor

  @spec start_link(list) :: tuple
  def start_link(args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: :"#{args[:otp_app]}_d#{args[:data_center]}")
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end


end
