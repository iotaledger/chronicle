defmodule Core.Supervisor.Conn do

  use Supervisor
  alias Core.Supervisor.Helper

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: :"#{args[:ring_key]}_#{args[:shard]}_#{args[:conn]}")
  end

  def init(args) do
    conn = args[:conn]
    shard = args[:shard]
    otp_app = args[:otp_app]
    address = args[:address]
    port = args[:port]
    priority = Application.get_env(:over_db, otp_app)[:__RECEIVER_PRIORITY__]
    reporter_per_shard = Application.get_env(:over_db, otp_app)[:__REPORTERS_PER_SHARD__]
    loggeds = Application.get_env(:over_db, otp_app)[:__LOGGED_PER_SHARD__]
    logged_args = [type: :logged, address: address, conn: conn, shard: shard, reporter_per_shard: reporter_per_shard, otp_app: otp_app]
    unloggeds = Application.get_env(:over_db, otp_app)[:__UNLOGGED_PER_SHARD__]
    unlogged_args = [type: :unlogged, address: address, conn: conn, shard: shard, reporter_per_shard: reporter_per_shard, otp_app: otp_app]
    counters = Application.get_env(:over_db, otp_app)[:__COUNTER_PER_SHARD__]
    counter_args = [type: :counter, address: address, conn: conn, shard: shard, reporter_per_shard: reporter_per_shard, otp_app: otp_app]
    reporter_args = [otp_app: otp_app, conn: conn, shard: shard, logged: loggeds, unlogged: unloggeds, counter: counters, address: address]
    sender_args = [otp_app: otp_app, conn: conn, shard: shard, address: address]
    recv_args = [otp_app: otp_app, conn: conn, shard: shard, address: address, port: port, priority: priority]
    Helper.gen_recv_children([],recv_args)
    |> Helper.gen_batcher_children(loggeds,logged_args)
    |> Helper.gen_batcher_children(unloggeds,unlogged_args) |> Helper.gen_batcher_children(counters,counter_args)
    |> Helper.gen_reporter_sender_children(reporter_per_shard,reporter_args, sender_args)
    |> :lists.reverse() |> Supervisor.init(strategy: :one_for_all)
  end


end
