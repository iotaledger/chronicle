defmodule Mix.Tasks.Compile.Broker do
  def run(_args) do
    erlang_path = :erlang.iolist_to_binary(:code.root_dir ++ '/erts-' ++ :erlang.system_info(:version) ++ '/include')
    broker_c_src_path = "apps/broker/c_src"
    # copy headers from erlang_path to broker_c_src_path
    File.cp_r(erlang_path, broker_c_src_path)
    # run the following command to build bazel
    {result, _errcode} = System.cmd("bazel", ["build", ":nifs.so"], cd: "apps/broker/c_src")
    # copy binary .so file to priv folder
    File.copy(broker_c_src_path <> "/bazel-bin/nifs.so", "apps/broker/priv/nifs.so")
    IO.binwrite(result)
  end
end

defmodule Broker.MixProject do
  use Mix.Project

  def project do
    [
      app: :broker,
      version: "0.1.0",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixir: "~> 1.8",
      make_executable: "make",
      make_makefile: "Makefile",
      start_permanent: Mix.env() == :prod,
      compilers: [:broker] ++ Mix.compilers,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :core],
      mod: {Broker.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:chumak, "~> 1.3"}
    ]
  end
end
