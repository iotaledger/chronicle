defmodule Core.Supervisor.Cluster.Manager do

  alias Core.Supervisor.Node
  use GenServer

  def start_link(args) do
    name = :"#{args[:otp_app]}_#{args[:data_center]}_manager"
    GenServer.start_link(__MODULE__, args, name: name)
  end

  def init(args) do
    Process.put(:sup_name, :"#{args[:otp_app]}_#{args[:data_center]}")
    send(self(), {:start_children, args})
    {:ok, args}
  end

  def handle_info({:start_children, args}, state) do
    # start_children
    otp_app = args[:otp_app]
    data_center = args[:data_center]
    Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__][data_center]
    |> start_children(otp_app, args)
    {:noreply, state}
  end

  defp start_children(nodes, otp_app, args) when is_list(nodes) do
    for {address, port} <- nodes do
      start_node_child(address, port, otp_app, args)
    end
  end

  defp start_children(_, _, _) do
    nil
  end

  defp start_node_child(address, port, otp_app, args) do
    args = [ [{:address, address}, {:port, port} | args] ]
    child = %{
      id: :"#{otp_app}_#{address}",
      start: {Node, :start_link, args }
    }
    {:ok, _} = DynamicSupervisor.start_child(Process.get(:sup_name), child)
  end


end
