defmodule ExtendedApiWeb.EndpointTest do
  use ExUnit.Case, async: true
  use Plug.Test

  @opts ExtendedApiWeb.Endpoint.init([])

  test "it retuns 400 with an invalid payload" do
    # Create a test connection
    conn = conn(:post, "/api", %{command?: "command?"})
    # Invoke the plug
    conn = ExtendedApiWeb.Endpoint.call(conn, @opts)
    assert conn.status == 400
  end

  test "it retuns 400 with an invalid, missing or empty_list hashes in getTrytes API call" do
    # Create a test connection and send query with empty
    conn = conn(:post, "/api", %{command: "getTrytes", hashes: []})
    # Invoke the plug
    conn = ExtendedApiWeb.Endpoint.call(conn, @opts)
    assert conn.status == 400
  end

  test "it retuns 200 with an a valid hashes in getTrytes API call" do
    # Create a test connection and send query with empty
    conn = conn(:post, "/api", %{command: "getTrytes", hashes: ["VALIDHASHE..."]})
    # Invoke the plug
    conn = ExtendedApiWeb.Endpoint.call(conn, @opts)
    assert conn.status == 200
  end

end
