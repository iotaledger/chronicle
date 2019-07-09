defmodule ExtendedApiWeb.CommandController do

  use ExtendedApiWeb, :controller
  alias ExtendedApi.Worker.GetTrytes
  alias ExtendedApi.Worker.FindTransactions.{Bundles, Addresses, Hints, Approvees, Tags}


  @doc """
    Show Function which handle "getTrytes" API call.
  """
  @spec show(Plug.Conn.t, map) :: Plug.Conn.t
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
          {:error, :invalid} ->
            # render json invaildType error.
            render_error(conn, "invalid.json")
          {:error, _} ->
            # something wrong happen (:dead_shard_stage,
            # scylla read_timeout error)
            render_error(conn, "something.json")
          :exit, _ ->
            render_error(conn, "timeout.json")
        end
      _ ->
        # respond error if hashes parameter is not provided.
        conn |> render_error("getTrytes.json")
    end
  end


  @doc """
    Show Function which handle "findTransactions" API call
    with bundles parameter.
  """
  @spec show(Plug.Conn.t, map) :: Plug.Conn.t
  def show(conn, %{"command" => "findTransactions", "bundles" => bundle_hashes}) do
    case bundle_hashes do
      [_|_] ->
        {:ok, pid} = Bundles.start_link()
        try do
          # await on the hashes result.
          # TODO: pass timeout
          response? = Bundles.await(pid, bundle_hashes)
          throw(response?)
        catch
          {:ok, hashes} ->
            # render json response.
            render(conn, "bundles.json", hashes: hashes)
          {:error, :invalid} ->
            # render json invaildType error.
            render_error(conn, "invalid.json")
          {:error, _} ->
            # something wrong happen (:dead_shard_stage,
            # scylla read_timeout error)
            render_error(conn, "something.json")
          :exit, _ ->
            render_error(conn, "timeout.json")
        end
      _ ->
        # respond error as parameters had invalid values.
        conn |> render_error("findTransactions.json")
    end
  end

  @doc """
    Show Function which handle "findTransactions" API call
    with addresses parameter.
  """
  @spec show(Plug.Conn.t, map) :: Plug.Conn.t
  def show(conn, %{"command" => "findTransactions", "addresses" => addresses}) do
    case addresses do
      [_|_] ->
        {:ok, pid} = Addresses.start_link()
        try do
          # await on the hashes/hints result.
          # TODO: pass timeout
          response? = Addresses.await(pid, addresses)
          throw(response?)
        catch
          {:ok, hashes, hints} ->
            # render json response.
            render(conn, "addresses.json", hashes: hashes, hints: hints)
          {:error, :invalid} ->
            # render json invaildType error.
            render_error(conn, "invalid.json")
          {:error, _} ->
            # something wrong happen (:dead_shard_stage,
            # scylla read_timeout error)
            render_error(conn, "something.json")
          :exit, _ ->
            render_error(conn, "timeout.json")
        end
      _ ->
        # respond error as parameters had invalid values.
        conn |> render_error("findTransactions.json")
    end
  end

  @doc """
    Show Function which handle "findTransactions" API call
    with hints parameter.
  """
  @spec show(Plug.Conn.t, map) :: Plug.Conn.t
  def show(conn, %{"command" => "findTransactions", "hints" => hints}) do
    case hints do
      [_|_] ->
        {:ok, pid} = Hints.start_link()
        try do
          # await on the hashes/hints result.
          # TODO: pass timeout
          response? = Hints.await(pid, hints)
          throw(response?)
        catch
          {:ok, hashes, hints} ->
            # render json response.
            render(conn, "addresses.json", hashes: hashes, hints: hints)
          {:error, :invalid} ->
            # render json invaildType error.
            render_error(conn, "invalid.json")
          {:error, _} ->
            # something wrong happen (:dead_shard_stage,
            # scylla read_timeout error)
            render_error(conn, "something.json")
          :exit, _ ->
            render_error(conn, "timeout.json")
        end
      _ ->
        # respond error as parameters had invalid values.
        conn |> render_error("findTransactions.json")
    end
  end

  @doc """
    Show Function which handle "findTransactions" API call
    with approvees parameter.
  """
  @spec show(Plug.Conn.t, map) :: Plug.Conn.t
  def show(conn, %{"command" => "findTransactions", "approvees" => approvees}) do
    case approvees do
      [_|_] ->
        {:ok, pid} = Approvees.start_link()
        try do
          # await on the hashes result.
          # TODO: pass timeout
          response? = Approvees.await(pid, approvees)
          throw(response?)
        catch
          {:ok, hashes} ->
            # render json response.
            render(conn, "approvees.json", hashes: hashes)
          {:error, :invalid} ->
            # render json invaildType error.
            render_error(conn, "invalid.json")
          {:error, _} ->
            # something wrong happen (:dead_shard_stage,
            # scylla read_timeout error)
            render_error(conn, "something.json")
          :exit, _ ->
            render_error(conn, "timeout.json")
        end
      _ ->
        # respond error as parameters had invalid values.
        conn |> render_error("findTransactions.json")
    end
  end

  @doc """
    Show Function which handle "findTransactions" API call
    with tags parameter.
  """
  @spec show(Plug.Conn.t, map) :: Plug.Conn.t
  def show(conn, %{"command" => "findTransactions", "tags" => tags}) do
    case tags do
      [_|_] ->
        {:ok, pid} = Tags.start_link()
        try do
          # await on the tag hints result.
          # TODO: pass timeout
          response? = Tags.await(pid, tags)
          throw(response?)
        catch
          {:ok, tag_hints} ->
            # render json response.
            render(conn, "tags.json", hints: tag_hints)
          {:error, :invalid} ->
            # render json invaild error.
            render_error(conn, "invalid.json")
          {:error, _} ->
            # something wrong happen (:dead_shard_stage,
            # scylla read_timeout error)
            render_error(conn, "something.json")
          :exit, _ ->
            render_error(conn, "timeout.json")
        end
      _ ->
        # respond error as parameters had invalid values.
        conn |> render_error("findTransactions.json")
    end
  end

  @doc """
    Show Function which handle undefined command
    in the API call.
  """
  @spec show(Plug.Conn.t, map) :: Plug.Conn.t
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
