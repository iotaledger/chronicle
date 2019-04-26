defmodule Core.Executor do

  @moduledoc """

    This is an Executor module, will be used by all
    the workers(core workers "if any" and other apps workers.)
    it gives them full access to the RING that is monitored
    by the core app.

  """

  use OverDB.Builder.Executor,
    otp_app: :core,
    ring: :core_ring

end
