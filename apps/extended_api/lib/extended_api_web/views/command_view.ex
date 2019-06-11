defmodule ExtendedApiWeb.CommandView do

  use ExtendedApiWeb, :view
  @derive Jason.Encoder

  def render("getTrytes.json", %{trytes: trytes}) do
    %{trytes: trytes}
  end

  def render("bundles.json", %{hashes: hashes}) do
    %{hashes: hashes}
  end

  def render("addresses.json", %{hashes: hashes, hints: hints}) do
    %{hashes: hashes, hints: hints}
  end

end
