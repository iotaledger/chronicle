defmodule ExtendedApiWeb.CommandController do

  use ExtendedApiWeb, :controller
  alias ExtendedApi.Worker.GetTrytes


  @doc """
    Show Function which handle "getTrytes" API call.
  """
  @spec show(map, map) :: Plug.Conn.t
  def show(conn, %{"command" => "getTrytes"} = params) do
    case params["hashes"] do
      [_|_] = hashes ->
        # start GetTrytes Worker.
        {:ok, pid} = GetTrytes.start_link()
        try do
          # await on the trytes result.
          # TODO: pass timeout
          response? = GetTrytes.await(pid, hashes)
          throw(response?)
        catch
          {:ok, trytes} ->
            # render json response.
            render(conn, "getTrytes.json", trytes: trytes)
          {:error, :invalid_type} ->
            # render json invaildType error.
            render_error(conn, "invalidType.json")
          {:error, err?} ->
            # something wrong happen (:dead_shard_stage,
            # scylla read_timeout error)
            render_error(conn, "something.json")
          :exit, value ->
            render_error(conn, "timout.json")
        end
      _ ->
        # respond error if hashes parameter is not provided.
        conn |> render_error("getTrytes.json")
    end
  end

  @doc """
    Show Function which handle undefined command
    in the API call.
  """
  @spec show(map, map) :: Plug.Conn.t
  def show(conn, _) do
    conn |> render_error("command.json")
  end

  @doc false
  defp render_error(conn, template) do
    conn
    |> put_status(400)
    |> put_view(ExtendedApiWeb.ErrorView)
    |> render(template)
    |> halt()
  end
end
