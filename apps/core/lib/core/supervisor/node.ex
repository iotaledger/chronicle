defmodule Core.Supervisor.Node do

  require Logger
  alias Core.Supervisor.Shard
  use Supervisor

  def start_link(args) do
    name = :"#{args[:otp_app]}_#{args[:address]}"
    Supervisor.start_link(__MODULE__, [{:ring_key, name}| args], name: name)
  end

  def init(args) do
    ring_key = args[:ring_key]
    shards = FastGlobal.get(ring_key)[:shards]
    children = my_children(shards, ring_key, args)
    Supervisor.init(children, strategy: :one_for_one)
  end

  defp my_children(shards, ring_key, args) when not is_nil(shards) do
    for shard <- 0..shards do
      %{id: :"#{ring_key}_#{shard}",
        start: {Shard, :start_link, [ [{:shard, shard} | args] ]} }
    end
  end

  defp my_children(_, ring_key, args) do
    Logger.warn("Node Supervisor sleeping for 5 seconds till the shards info regarding #{args[:ring_key]} is available.")
    Process.sleep(5000)
    FastGlobal.get(args[:ring_key])[:shards] |> my_children(ring_key, args)
  end

end
