defmodule Broker.Collector.Validator do

  @max_demand Application.get_env(:broker, :__MAX_DEMAND__) || 64
  @min_demand Application.get_env(:broker, :__MIN_DEMAND__) || 0

  use GenStage
  require Logger

  @spec start_link(Keyword.t) :: tuple
  def start_link(args) do
    name = name_by_topic?(args)
    GenStage.start_link(__MODULE__, [name: name] ++ args, name: name)
  end

  @spec init(Keyword.t) :: tuple
  def init(args) do
    # put name
    Process.put(:name, args[:name])
    partitions = args[:transaction_partitions]
    dispatch_fn = fn(event) -> {event, :erlang.phash2(elem(event,0), partitions)} end
    opts = [
      # this option make Validator act as partitionDispatcher to tx Collectors
      dispatcher: {GenStage.PartitionDispatcher, partitions: partitions,
        hash: dispatch_fn},
      # this option to subscribe to Validator producer(distributor)
      subscribe_to: [{:tx_distributor, max_demand: @max_demand, min_demand: @min_demand}]
    ]
    {:producer_consumer, %{producer: nil}, opts}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:producer, _, from, state) do
    # we add producer(distributor from_reference to be able to send ask requests)
    state = %{state | producer: from}
    Logger.info("Validator: #{Process.get(:name)} got subscribed_to Distributor")
    {:automatic, state}
  end

  @spec handle_subscribe(atom, tuple | list, tuple, map) :: tuple
  def handle_subscribe(:consumer, _, _from, state) do
    Logger.info("Validator: #{Process.get(:name)} got subscribed_to Transaction Collector")
    # we keep it automatic to dispatch events on the fly
    {:automatic, state}
  end

  @doc """
    Handle events from producer(distributor)
    - validate the events(txs)
    - ask for max_demand
    - send as events to tx_collector(s)
  """
  @spec handle_events(list, tuple, map) :: tuple
  def handle_events(events, _from, state) do
    # process the events and return list of booleans
    events_status = process_events(events)
    # extract_valid events only
    events = extract_valid(events_status,events)
    # pass events to tx_collector(s)
    {:noreply, events, state}
  end

  def handle_info(:ask, state) do
    GenStage.ask(state[:producer], @max_demand)
    {:noreply,[],state}
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

  # private functions to process the events
  defp process_events(events,hashes\\<<>>,
      p0\\<<>>,p1\\<<>>,p2\\<<>>,p3\\<<>>,p4\\<<>>,p5\\<<>>,
      p6\\<<>>,p7\\<<>>,p8\\<<>>,p9\\<<>>,p10\\<<>>,p11\\<<>>,p12\\<<>>,
      p13\\<<>>,p14\\<<>>,p15\\<<>>,p16\\<<>>,p17\\<<>>,p18\\<<>>,p19\\<<>>,
      p20\\<<>>,p21\\<<>>,p22\\<<>>,p23\\<<>>,p24\\<<>>,p25\\<<>>,p26\\<<>>,
      p27\\<<>>,p28\\<<>>,p29\\<<>>,p30\\<<>>,p31\\<<>>,p32\\<<>>)
  defp process_events([{hash, <<c0::81-bytes,c1::81-bytes,
      c2::81-bytes,c3::81-bytes,c4::81-bytes,c5::81-bytes,c6::81-bytes,c7::81-bytes,
      c8::81-bytes,c9::81-bytes,c10::81-bytes,c11::81-bytes,c12::81-bytes,
      c13::81-bytes,c14::81-bytes,c15::81-bytes,c16::81-bytes,c17::81-bytes,
      c18::81-bytes,c19::81-bytes,c20::81-bytes,c21::81-bytes,c22::81-bytes,
      c23::81-bytes,c24::81-bytes,c25::81-bytes,c26::81-bytes,c27::81-bytes,
      c28::81-bytes,c29::81-bytes,c30::81-bytes,c31::81-bytes,c32::81-bytes>>,_nil}
      | rest],hashes,p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14,p15,p16,p17,
      p18,p19,p20,p21,p22,p23,p24,p25,p26,p27,p28,p29,p30,p31,p32) do
        # keep processing events
        process_events(rest,hashes<>hash,p0<>c0,p1<>c1,p2<>c2,p3<>c3,p4<>c4,
          p5<>c5,p6<>c6,p7<>c7,p8<>c8,p9<>c9,p10<>c10,p11<>c11,p12<>c12,p13<>c13,
          p14<>c14,p15<>c15,p16<>c16,p17<>c17,p18<>c18,p19<>c19,p20<>c20,p21<>c21,
          p22<>c22,p23<>c23,p24<>c24,p25<>c25,p26<>c26,p27<>c27,p28<>c28,p29<>c29,
          p30<>c30,p31<>c31,p32<>c32)
  end

  defp process_events([], hashes, p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,
      p11,p12,p13,p14,p15,p16,p17,p18,p19,p20,p21,p22,p23,p24,p25,p26,p27,
      p28,p29,p30,p31,p32) do
        # get the length(tx_count) of events , we can do that by checking the length of hashes:
        tx_count = div(byte_size(hashes),81)
        # now we have the completed 0..32 chunks
        # therefore we should call add_trytes and absorb for each part(p0..p32)
        # ############## init curl_p ################
        {:ok, pecurl} = Nifs.curl_p_init()
        # ####### add_trytes and absorb #############
        # add_trytes for p0
        Nifs.add_trytes(pecurl,tx_count,p0)
        # absorb p0
        Nifs.absorb(pecurl)
        # add_trytes for p1
        Nifs.add_trytes(pecurl,tx_count,p1)
        # absorb p1
        Nifs.absorb(pecurl)
        # add_trytes for p2
        Nifs.add_trytes(pecurl,tx_count,p2)
        # absorb p2
        Nifs.absorb(pecurl)
        # add_trytes for p3
        Nifs.add_trytes(pecurl,tx_count,p3)
        # absorb p3
        Nifs.absorb(pecurl)
        # add_trytes for p4
        Nifs.add_trytes(pecurl,tx_count,p4)
        # absorb p4
        Nifs.absorb(pecurl)
        # add_trytes for p5
        Nifs.add_trytes(pecurl,tx_count,p5)
        # absorb p5
        Nifs.absorb(pecurl)
        # add_trytes for p6
        Nifs.add_trytes(pecurl,tx_count,p6)
        # absorb p6
        Nifs.absorb(pecurl)
        # add_trytes for p7
        Nifs.add_trytes(pecurl,tx_count,p7)
        # absorb p7
        Nifs.absorb(pecurl)
        # add_trytes for p8
        Nifs.add_trytes(pecurl,tx_count,p8)
        # absorb p8
        Nifs.absorb(pecurl)
        # add_trytes for p9
        Nifs.add_trytes(pecurl,tx_count,p9)
        # absorb p9
        Nifs.absorb(pecurl)
        # add_trytes for p10
        Nifs.add_trytes(pecurl,tx_count,p10)
        # absorb p10
        Nifs.absorb(pecurl)
        # add_trytes for p11
        Nifs.add_trytes(pecurl,tx_count,p11)
        # absorb p11
        Nifs.absorb(pecurl)
        # add_trytes for p12
        Nifs.add_trytes(pecurl,tx_count,p12)
        # absorb p12
        Nifs.absorb(pecurl)
        # add_trytes for p13
        Nifs.add_trytes(pecurl,tx_count,p13)
        # absorb p13
        Nifs.absorb(pecurl)
        # add_trytes for p14
        Nifs.add_trytes(pecurl,tx_count,p14)
        # absorb p14
        Nifs.absorb(pecurl)
        # add_trytes for p15
        Nifs.add_trytes(pecurl,tx_count,p15)
        # absorb p15
        Nifs.absorb(pecurl)
        # add_trytes for p16
        Nifs.add_trytes(pecurl,tx_count,p16)
        # absorb p16
        Nifs.absorb(pecurl)
        # add_trytes for p17
        Nifs.add_trytes(pecurl,tx_count,p17)
        # absorb p17
        Nifs.absorb(pecurl)
        # add_trytes for p18
        Nifs.add_trytes(pecurl,tx_count,p18)
        # absorb p18
        Nifs.absorb(pecurl)
        # add_trytes for p19
        Nifs.add_trytes(pecurl,tx_count,p19)
        # absorb p19
        Nifs.absorb(pecurl)
        # add_trytes for p20
        Nifs.add_trytes(pecurl,tx_count,p20)
        # absorb p20
        Nifs.absorb(pecurl)
        # add_trytes for p21
        Nifs.add_trytes(pecurl,tx_count,p21)
        # absorb p21
        Nifs.absorb(pecurl)
        # add_trytes for p22
        Nifs.add_trytes(pecurl,tx_count,p22)
        # absorb p22
        Nifs.absorb(pecurl)
        # add_trytes for p23
        Nifs.add_trytes(pecurl,tx_count,p23)
        # absorb p23
        Nifs.absorb(pecurl)
        # add_trytes for p24
        Nifs.add_trytes(pecurl,tx_count,p24)
        # absorb p24
        Nifs.absorb(pecurl)
        # add_trytes for p25
        Nifs.add_trytes(pecurl,tx_count,p25)
        # absorb p25
        Nifs.absorb(pecurl)
        # add_trytes for p26
        Nifs.add_trytes(pecurl,tx_count,p26)
        # absorb p26
        Nifs.absorb(pecurl)
        # add_trytes for p27
        Nifs.add_trytes(pecurl,tx_count,p27)
        # absorb p27
        Nifs.absorb(pecurl)
        # add_trytes for p28
        Nifs.add_trytes(pecurl,tx_count,p28)
        # absorb p28
        Nifs.absorb(pecurl)
        # add_trytes for p29
        Nifs.add_trytes(pecurl,tx_count,p29)
        # absorb p29
        Nifs.absorb(pecurl)
        # add_trytes for p30
        Nifs.add_trytes(pecurl,tx_count,p30)
        # absorb p30
        Nifs.absorb(pecurl)
        # add_trytes for p31
        Nifs.add_trytes(pecurl,tx_count,p31)
        # absorb p31
        Nifs.absorb(pecurl)
        # add_trytes for p32
        Nifs.add_trytes(pecurl,tx_count,p32)
        # absorb p32
        Nifs.absorb(pecurl)
        ############ squeeze #############
        Nifs.squeeze(pecurl)
        ############ get trytes and compare hashes, should return [bool] #############
        Nifs.get_status(pecurl, tx_count, hashes)
  end

  defp extract_valid(events_status, events, valid_events_acc \\ [])
  defp extract_valid([event_status | rest_status],[event | rest_events], valid_events_acc) do
    valid_events_acc =
      if event_status do
        [event | valid_events_acc]
      else
        valid_events_acc
      end
    extract_valid(rest_status,rest_events,valid_events_acc)
  end

  defp extract_valid([],[], valid_events_acc) do
    valid_events_acc
  end

  # generate the validator name by topic name
  defp name_by_topic?(args) do
    case args[:topic] do
      :tx_trytes ->
        :"tv#{args[:num]}"
      :sn_trytes ->
        :"sv#{args[:num]}"
    end
  end

end
