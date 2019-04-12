defmodule Core.Helper do

  @moduledoc false

  alias OverDB.Engine.{Batcher, Reporter, Sender, Receiver}

  @spec gen_batcher_children(list, integer, list) :: list
  def gen_batcher_children(acc,b_num,args) when is_list(acc) do
    if b_num do
        for b <- 1..b_num do
          %{
              id: rand_child_id(),
              start: {Batcher, :start_link, [[{:num, b} | args]]}
            }
        end ++ acc
    else
      acc
    end
  end

  @spec gen_reporter_sender_children(list, integer, list, list) :: list
  def gen_reporter_sender_children(acc, reporter_per_shard, reporter_args, sender_args) when is_list(acc) do
    stream_ranges = OverDB.Engine.Helper.gen_stream_ids(reporter_per_shard)
    Enum.reduce(stream_ranges, acc, fn({p, r}, acc) ->
      reporter_child = %{
          id: rand_child_id(),
          start: {Reporter, :start_link,[ [{:range, r}, {:partition, p} | reporter_args] ] }
        }
      sender_child = %{
          id: rand_child_id(),
          start: {Sender, :start_link, [ [{:partition, p} | sender_args] ]}
        }
      [sender_child, reporter_child | acc]
     end)
  end

  @spec gen_recv_children(list, list) :: list
  def gen_recv_children(acc,recv_args) when is_list(acc) do
    recv_child = %{
        id: rand_child_id(),
        start: {Receiver, :start_link, [recv_args]}
      }
    [recv_child | acc]
  end

  @spec rand_child_id() :: integer
  def rand_child_id() do
    :rand.uniform(9999999999)
  end

end
