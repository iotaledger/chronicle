defmodule Broker.Collector.BundleValidator do
  use GenStage
  alias Broker.Collector.Inserter

  def start_link(args) do
    name = name_by_topic?(args)
    args = Keyword.put(args, :name, name)
    GenStage.start_link(__MODULE__, args,name: name)
  end

  def init(args) do
    Process.put(:name, args[:name])
    opts = [
      subscribe_to: [producer_name_by_topic?(args)]
    ]
    {:consumer, [], opts}
  end

  def handle_events(bundles, _from, state) do
    process_bundles(bundles)
    {:noreply, [], state}
  end

  defp process_bundles([bundle | rest]) do
    process_bundle(bundle)
    process_bundles(rest)
  end

  defp process_bundles([]) do
    :ok
  end

  defp process_bundle(bundle) do
    # create bundle_trytes from :trytes key inside each tx in bundle
    bundle_trytes =
      for %{trytes: trytes} <- Enum.reverse(bundle), into: "", do: trytes
    # bundle length
    bundle_length = length(bundle)
    if Nifs.validate_bundle(bundle_length, bundle_trytes) do
      # insert bundle by spawn genserver inserter
      Inserter.start_link(bundle)
    end
  end

  # generate the validator name by topic name
  defp name_by_topic?(args) do
    case args[:topic] do
      :tx_trytes ->
        :"tbv#{args[:num]}"
      :sn_trytes ->
        :"sbv#{args[:num]}"
    end
  end

  def producer_name_by_topic?(args) do
    case args[:topic] do
      :tx_trytes ->
        :"tbc#{args[:num]}"
      :sn_trytes ->
        :"sbc#{args[:num]}"
    end
  end

  def child_spec(args) do
    %{
      id: name_by_topic?(args),
      start: {__MODULE__, :start_link, [args]},
      type: :worker,
      restart: :permanent,
      shutdown: 500
    }
  end

end
