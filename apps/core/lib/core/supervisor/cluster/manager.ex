defmodule Core.Supervisor.Cluster.Manager do

  @moduledoc """
    This the Cluster Manager who start dynamic children(nodes)
    on behalf dynamicCluster supervisor.

    # NOTE: This Manager is supposed to add(start) or remove(kill)
    nodes when the topology change.
  """

  alias Core.Supervisor.Node
  use GenServer

  @doc """
   this start_link function which start the genserver manager
   and register the following name :otpapp_datacenter_manager.
   where otpapp is the app name (eg core) and datacenter is
   the datacenter name(eg dc1, fullname: :core_dc1_manager).
  """
  @spec start_link(list) :: tuple
  def start_link(args) do
    name = :"#{args[:otp_app]}_#{args[:data_center]}_manager"
    GenServer.start_link(__MODULE__, args, name: name)
  end

  @doc """
   this init function which put the supervisor name in the
   processor global state(dict), then the processor send self()
   msg {:start_children, args} to start the initial nodes
   supervisors
  """
  @spec init(list) :: tuple
  def init(args) do
    Process.put(:sup_name, :"#{args[:otp_app]}_d#{args[:data_center]}")
    send(self(), {:start_children, args})
    {:ok, args}
  end

  @doc """
   this handle_info function which handle the {:start_children, _}
   msgs, and get the required nodes from the config.exs file
   to start the supervisor for each node.
  """
  @spec handle_info(tuple, term) :: tuple
  def handle_info({:start_children, args}, state) do
    # start_children
    otp_app = args[:otp_app]
    data_center = args[:data_center]
    Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__][data_center]
    |> start_children(otp_app, args)
    {:noreply, state}
  end

  @doc """
    this private function which takes nodes list and otp_app
    with thier arguments.
  """
  @spec start_children(list, atom, list) :: list
  defp start_children(nodes, otp_app, args) when is_list(nodes) do
    for {address, port} <- nodes do
      start_node_child(address, port, otp_app, args)
    end
  end

  @doc """
    this private function which act as an error handler.
  """
  @spec start_children(term, term, term) :: nil
  defp start_children(_, _, _) do
    nil
  end

  @doc """
    this private function which invoke the DynamicCluster
    supervisor to start a given node.
  """
  @spec start_node_child(list, integer, atom, list) :: tuple
  defp start_node_child(address, port, otp_app, args) do
    args = [ [{:address, address}, {:port, port} | args] ]
    child = %{
      id: :"#{otp_app}_#{address}",
      start: {Node, :start_link, args }
    }
    {:ok, _} = DynamicSupervisor.start_child(Process.get(:sup_name), child)
  end


end
