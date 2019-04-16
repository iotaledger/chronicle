defmodule Core.Supervisor.DynamicCluster do

  use DynamicSupervisor

  def start_link(args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: :"#{args[:otp_app]}_#{args[:data_center]}")
  end

  @impl true
  def init(_) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end


end
