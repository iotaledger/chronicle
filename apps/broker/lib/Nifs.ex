defmodule Nifs do
  @on_load :init

  def init do
    :ok = :erlang.load_nif(Application.app_dir(:broker) <> "/priv/nifs", 0)
  end

  def curl_p_init() do
    exit(:nif_library_not_loaded)
  end

  def absorb(_) do
    exit(:nif_library_not_loaded)
  end

  def squeeze(_) do
    exit(:nif_library_not_loaded)
  end

  def add_trytes(_,_,_) do
    exit(:nif_library_not_loaded)
  end
  def get_trytes(_,_,_) do
    exit(:nif_library_not_loaded)
  end
end
