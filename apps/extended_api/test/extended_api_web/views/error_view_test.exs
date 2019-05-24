defmodule ExtendedApiWeb.ErrorViewTest do
  use ExtendedApiWeb.ConnCase, async: true

  # Bring render/3 and render_to_string/3 for testing custom views
  import Phoenix.View

  test "renders command.json" do
    assert render(ExtendedApiWeb.ErrorView, "command.json", []) ==
      %{error: "'command' parameter has not been specified"}
  end

  test "renders getTrytes.json" do
    assert render(ExtendedApiWeb.ErrorView, "getTrytes.json", []) ==
      %{error: "parameter: hashes is not provided"}
  end
end
