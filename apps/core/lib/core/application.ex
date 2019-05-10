defmodule Core.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  alias OverDB.{Ring.Monitor, Engine.Preparer}
  use Application

  def start(_type, _args) do
    otp_app = Application.get_application(__MODULE__)
    {dc, _} = Application.get_env(:over_db, otp_app)[:__DATA_CENTERS__] |> hd()
    dc1_args = [otp_app: :core, data_center: dc]
    children = [
      {Monitor, [otp_app: otp_app]},
      {Preparer, [otp_app: otp_app]},
    {Core.Supervisor.Cluster, dc1_args}
    ]
    opts = [strategy: :one_for_one, name: :core]
    Supervisor.start_link(children, opts)
  end

end
