defmodule ExtendedApiWeb.Router do
  use ExtendedApiWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/api", ExtendedApiWeb do
    pipe_through :api
    post "/", CommandController, :show
  end
end
