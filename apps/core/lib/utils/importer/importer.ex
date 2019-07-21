defmodule Core.Utils.Importer do

  require Logger
  alias Core.Utils.Converter
  alias Core.Utils.Importer.Worker
  @initial_acc %{index: nil, bundle: []}


  def run(path) when is_binary(path) do
    Logger.info("DISPATCHER IN PROCESS")
    File.stream!(path)
    |> Flow.from_enumerable(max_demand: 1)
    |> Flow.partition(stages: 1)
    |> Flow.reduce(fn -> @initial_acc end, fn (line, acc) ->
         # line = String.trim(line, "\n")
         # logic to collect all the transactions(lines) for a given bundle.
         # NOTE: this only works with ordered dmp file.
         # - Put transaction line in map.
         transaction = Converter.line_to_tx_object(line)
         # check acc.
         case acc do
           %{index: nil, bundle: bundle}->
               # logic to start collecting new bundle.
               case transaction do
                %{last_index: 0} ->
                  # send bundle([transaction]) to OverDB worker.
                  bundle = [transaction | bundle] # note length = 1.
                  # Worker
                  Worker.start_link(bundle)
                  @initial_acc
                %{last_index: last_index} ->
                  # update acc
                  %{acc | index: last_index, bundle: [transaction | bundle]}
               end
            %{index: index, bundle: bundle} ->
                case index-1 do
                  0 ->
                    # send bundle([transaction]) to OverDB worker.
                    bundle = [transaction | bundle]
                    # Worker
                    Worker.start_link(bundle)
                    @initial_acc
                  index ->
                    # update acc
                    %{acc | index: index, bundle: [transaction | bundle]}
                end
          end
       end
       )
    |> Flow.run()
  end


end
