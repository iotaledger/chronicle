defmodule Core.Utils.Importer do

  require Logger
  use GenServer
  alias Core.Utils.Converter
  alias Core.Utils.Importer.{Worker27,Worker81}
  @max Application.get_env(:core, :max_import_concurrent) || 1000

  def start_link(_args) do
    Logger.info("Starting the importing script")
    GenServer.start_link(__MODULE__,%{dmps: [], max: @max, pending: 0},name: :importer)
  end

  def init(state) do
    dmps = File.read!("./historical/dmps.txt")
    |> String.split("\n", trim: true)
    Process.sleep(30000)
    Logger.info("Importer started")
    send(self(), :import)
    state = Map.put(state, :dmps, dmps)
    {:ok, state}
  end

  def handle_info(:finished_one, %{pending: pending}= state) do
    state = process_more?(%{state | pending: pending-1})
    {:noreply, state}
  end

  def handle_info(:import, %{dmps: [h | _t]} = state) do
    snapshot_index = String.to_integer(h)
    state = import_dmp(snapshot_index,state)
    send(self(),{state[:current?], false})
    {:noreply, state}
  end

  def handle_info(:import, %{dmps: []} = state) do
    Logger.info("No dmps to import")
    {:noreply, state}
  end

  def handle_info({:fetch_bundle_27, finished?},%{file: file, max: max, pending: pending} = state) do
    state =
      case fetch_bundle_27(file) do
        bundle when is_list(bundle) ->
          process_bundle_27(bundle)
          pending = pending+1
          if !finished? and pending < max  do
            send(self(), {:fetch_bundle_27, false})
          end
          %{state | pending: pending}
        :eof ->
          %{state | current?: true}
      end
    {:noreply, state}
  end

  def handle_info({:fetch_bundle_81, finished?},%{file: file, max: max, pending: pending} = state) do
    state =
      case fetch_bundle_81(file) do
        bundle when is_list(bundle) ->
          process_bundle_81(bundle)
          pending = pending+1
          if !finished? and pending < max  do
            send(self(), {:fetch_bundle_81, false})
          end
          %{state | pending: pending}
        :eof ->
          %{state | current?: true}
      end
    {:noreply, state}
  end

  def handle_info({:fetch_bundle_81_from_list, finished?}, %{file: bundles, max: max, pending: pending} = state) do
    state =
      case fetch_bundle_81_from_list(bundles) do
        bundle when is_list(bundle) ->
          process_bundle_81(bundle)
          pending = pending+1
          if !finished? and pending < max  do
            send(self(), {:fetch_bundle_81_from_list, false})
          end
          [_ | bundles] = bundles
          %{state | pending: pending, file: bundles}
        :eof ->
          %{state | current?: true}
      end
    {:noreply, state}
  end

  # process the unordered and only confirmed dmps
  # note: the nonce is 81-trytes version
  defp import_dmp(snapshot_index,state) when snapshot_index in [6000, 13157, 18675] do
    file = File.stream!("./historical/data/#{snapshot_index}.dmp")
    Logger.info("Importing #{snapshot_index} file")
    transactions =
      for <<hash::81-bytes,_,_trytes::binary>> = line <- file ,into: %{} do
        {hash, Converter.line_81_to_tx_object(line, snapshot_index)}
      end
    bundles = get_bundles_from_transactions_map(transactions)
    Map.put(Map.put(state, :file, bundles), :current?, :fetch_bundle_81_from_list)
  end

  # process the ordered and vaild dmps.
  # note: the nonce is 81-trytes version
  defp import_dmp(snapshot_index,state) when snapshot_index < 242662 do
    # process the file now
    Logger.info("Importing #{snapshot_index} file")
    # first we open the dmp
    file = File.open!("./historical/data/#{snapshot_index}.dmp")
    Map.put(Map.put(state, :file, file), :current?, :fetch_bundle_81)
  end

  # process the ordered and vaild dmps.
  # note: the nonce is 27-trytes version
  defp import_dmp(snapshot_index,state) when snapshot_index >= 242662 do
    # process the file now
    Logger.info("Importing #{snapshot_index} file")
    # first we open the dmp
    file = File.open!("./historical/data/#{snapshot_index}.dmp")
    Map.put(Map.put(state, :file, file), :current?, :fetch_bundle_27)
  end

  defp fetch_remaning_txs_in_bundle(0,_,_, bundle) do
    bundle
  end
  defp fetch_remaning_txs_in_bundle(lx,transactions,trunk, bundle) do
    %{trunk: trunk} = tx = transactions[trunk]
    fetch_remaning_txs_in_bundle(lx-1,transactions,trunk,[tx | bundle])
  end

  defp get_bundles_from_transactions_map(transactions) do
    Enum.reduce(transactions,[[]], fn({_, %{current_index: cx, last_index: lx, trunk: trunk} = tx }, acc) ->
      case cx do
        0 ->
          if lx == 0 do
            [[tx]] ++ acc
          else
            bundle = fetch_remaning_txs_in_bundle(lx,transactions,trunk,[tx])
            [bundle | acc]
          end
        _ ->
         acc
      end
    end)
  end

  def fetch_bundle_27(file, bundle_acc \\ []) do
    case IO.read(file, :line) do
      :eof ->
        :eof
      line when is_binary(line) ->
        %{current_index: current_index, last_index: last_index} = transaction = Converter.line_to_tx_object(line)
        # when current_index != last_index mean there is more transactions to fetch
        if current_index == last_index do
          # we put the transaction in the bundle, and return the bundle
          [transaction | bundle_acc]
        else
          # fetch one more transaction(line)
          fetch_bundle_27(file,[transaction | bundle_acc])
        end
    end
  end

  def fetch_bundle_81(file, bundle_acc \\ []) do
    case IO.read(file, :line) do
      :eof ->
        :eof
      line when is_binary(line) ->
        %{current_index: current_index, last_index: last_index} = tx = Converter.line_81_to_tx_object(line)
        # when current_index != last_index mean there is more transactions to fetch
        if current_index == last_index do
          # we put the transaction in the bundle, and return the bundle
          [tx | bundle_acc]
        else
          # fetch one more transaction(line)
          fetch_bundle_81(file,[tx | bundle_acc])
        end
    end
  end

  def fetch_bundle_81_from_list(bundles) do
    case bundles do
      [] ->
        :eof
      [bundle | _] ->
        bundle
    end
  end

  def process_more?(%{pending: pending, current?: true} = state) do
    if pending != 1 do
      # we still have more pending bundles, so we should wait and return state
      state
    else
      # finished the dmp, therefore we should process the remaining dmps(if any) and log the result.
      # and update the status.txt file and dmps state, finally we return state
      process_dmp?(state)
    end
  end

  # common functions
  def process_more?(%{current?: current?}= state) do
    # first we send self msg to fetch more bundle
    send(self(),{current?, true})
    # return state
    state
  end

  def process_dmp?(%{dmps: [dmp | rest] = dmps} = state) when length(dmps) > 1 do
    if String.to_integer(dmp) > 18675 do
      File.close(state[:file])
    end
    Logger.info("Imported #{dmp}")
    # update the dmps text file
    File.write("./historical/dmps.txt", Enum.join(rest, "\n"))
    send(self(),:import)
    # we put the new dmps txt state in the state.
    %{state | dmps: rest}
  end

  def process_dmp?(%{dmps: [dmp]} = state)  do
    Logger.info("Imported the last dmp file #{dmp}")
    # update the dmps text file
    File.write("./historical/dmps.txt", "")
    # we put the new dmps txt state in the state.
    %{state | dmps: []}
  end

  def process_bundle_27(bundle) do
    Worker27.start_link(bundle)
  end

  def process_bundle_81(bundle) do
    Worker81.start_link(bundle)
  end
end
