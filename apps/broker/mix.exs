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
      compilers: [:elixir_make] ++ Mix.compilers,
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
      {:chumak, "~> 1.3"},
      {:elixir_make, "~> 0.6", runtime: false}
    ]
  end
end
