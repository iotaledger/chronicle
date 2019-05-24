defmodule ExtendedApiWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :extended_api

  socket "/socket", ExtendedApiWeb.UserSocket,
    websocket: true,
    longpoll: false

  plug Plug.RequestId
  plug Plug.Logger

  plug Plug.Parsers,
    parsers: [:json],
    pass: ["*/*"],
    json_decoder: Phoenix.json_library()

  plug Plug.MethodOverride
  plug Plug.Head

  plug ExtendedApiWeb.Router
end
