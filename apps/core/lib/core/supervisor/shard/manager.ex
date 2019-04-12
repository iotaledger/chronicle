defmodule Core.Supervisor.Shard.Manager do

  alias Core.Supervisor.Conn
  use GenServer

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: :"#{args[:ring_key]}_#{args[:shard]}_manager")
  end

  def init(args) do
    Process.put(:sup_name, :"#{args[:ring_key]}_d#{args[:shard]}")
    send(self(), {:start_children, args})
    {:ok, args}
  end

  def handle_info({:start_children, args}, state) do
    Application.get_env(:over_db, args[:otp_app])[:__CONNS_PER_SHARD__]
    |> start_children(args)
    {:noreply, state}
  end

  defp start_children(conns,args) when conns > 0 do
    shard = args[:shard]
    for conn <- 1..conns do
      start_conn_child(shard, conn, args)
    end
  end

  defp start_children(_, _) do
    :empty
  end

  defp start_conn_child(shard, conn, args) do
    child = %{
      id: :"#{args[:ring_key]}_#{shard}_#{conn}",
      start: {Conn, :start_link, [ [{:conn, conn} | args] ]}
    }
    {:ok, _} = DynamicSupervisor.start_child(Process.get(:sup_name), child)
  end

end
