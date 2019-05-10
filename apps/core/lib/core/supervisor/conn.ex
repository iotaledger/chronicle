defmodule Core.Supervisor.Conn do

  @moduledoc """
    This the Conn supervisor which supervise all the
    lighweight processors that corrospond to
    his shard's stage.
    (batchers, reporters, senders, receiver).
  """

  use Supervisor
  alias Core.Supervisor.Helper

  @spec start_link(list) :: tuple
  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: :"#{args[:ring_key]}_#{args[:shard]}_#{args[:conn]}")
  end

  @spec init(list) :: tuple
  def init(args) do
    conn = args[:conn] # fetching the conn num
    shard = args[:shard] # fetching the shard num
    otp_app = args[:otp_app] # fetching the otp_app name
    address = args[:address] # fetching the ip_address
    port = args[:port] # fetching the port.
    # fetching the receiver priority
    priority = Application.get_env(:over_db, otp_app)[:__RECEIVER_PRIORITY__] || :normal
    # fetching the reporters num
    reporter_per_shard = Application.get_env(:over_db, otp_app)[:__REPORTERS_PER_SHARD__]
    # creating batcher_args.
    batcher_args = [address: address, conn: conn, shard: shard, reporter_per_shard: reporter_per_shard, otp_app: otp_app]
    # fetching the logged batchers num
    loggeds = Application.get_env(:over_db, otp_app)[:__LOGGED_PER_SHARD__]
    # fetching the unlogged batchers num
    unloggeds = Application.get_env(:over_db, otp_app)[:__UNLOGGED_PER_SHARD__]
    # fetching the counter batchers num
    counters = Application.get_env(:over_db, otp_app)[:__COUNTER_PER_SHARD__]
    # creating reporter_args.
    reporter_args = [otp_app: otp_app, conn: conn, shard: shard, logged: loggeds, unlogged: unloggeds, counter: counters, address: address]
    # creating sender_args.
    sender_args = [otp_app: otp_app, conn: conn, shard: shard, address: address]
    # creating recv_args.
    recv_args = [otp_app: otp_app, conn: conn, shard: shard, address: address, port: port, priority: priority]
    # generating children list and then starting Conn supervisor with one for all strategy.
    Helper.gen_recv_children([],recv_args)
    |> Helper.gen_batcher_children(loggeds,[{:type, :logged} | batcher_args])
    |> Helper.gen_batcher_children(unloggeds,[{:type, :unlogged} | batcher_args])
    |> Helper.gen_batcher_children(counters,[{:type, :counter} | batcher_args])
    |> Helper.gen_reporter_sender_children(reporter_per_shard,reporter_args, sender_args)
    |> :lists.reverse() |> Supervisor.init(strategy: :one_for_all)
  end


end
