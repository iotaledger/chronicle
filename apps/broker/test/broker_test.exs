defmodule BrokerTest do
  use ExUnit.Case
  doctest Broker

  test "greets the world" do
    assert Broker.hello() == :world
  end
end
