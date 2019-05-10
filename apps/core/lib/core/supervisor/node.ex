defmodule Core.Supervisor.Node do

  @moduledoc """
    This the Node supervisor which supervise the Shard
    supervisor.
  """

  require Logger
  alias Core.Supervisor.Shard
  use Supervisor

  @spec start_link(list) :: tuple
  def start_link(args) do
    name = :"#{args[:otp_app]}_#{args[:address]}"
    Supervisor.start_link(__MODULE__, [{:ring_key, name}| args], name: name)
  end

  @spec init(list) :: tuple
  def init(args) do
    ring_key = args[:ring_key]
    # it assumes the shards info is available in
    # the global ring, so it try to fetch it and
    # pass it to my_children() function
    shards = FastGlobal.get(ring_key)[:shards]
    children = my_children(shards, ring_key, args)
    Supervisor.init(children, strategy: :one_for_one)
  end

  @spec my_children(integer, atom, list) :: list
  defp my_children(shards, ring_key, args) when not is_nil(shards) do
    for shard <- 0..shards do
      %{id: :"#{ring_key}_#{shard}",
        start: {Shard, :start_link, [ [{:shard, shard} | args] ]} }
    end
  end

  @spec ny_children(nil, atom, list) :: term
  defp my_children(_, ring_key, args) do
    Logger.warn("Node Supervisor sleeping for 5 seconds till the shards info regarding #{args[:ring_key]} is available.")
    Process.sleep(5000)
    FastGlobal.get(args[:ring_key])[:shards] |> my_children(ring_key, args)
  end

end
