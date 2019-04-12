defmodule CoreTest do
  use ExUnit.Case
  doctest Core

  test "Starting Application" do
    assert Application.started_applications()
  end
end
