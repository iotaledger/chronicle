defmodule Core.Utils.Converter do

  @moduledoc """
    Pure elixir trytes/trits/integer converter.
  """
  alias Core.Utils.Struct.Transaction

  @trytesAlphabet "9ABCDEFGHIJKLMNOPQRSTUVWXYZ" # All possible tryte values

  # map of all trits representations
  @trytesTrits [
    [ 0,  0,  0],
    [ 1,  0,  0],
    [-1,  1,  0],
    [ 0,  1,  0],
    [ 1,  1,  0],
    [-1, -1,  1],
    [ 0, -1,  1],
    [ 1, -1,  1],
    [-1,  0,  1],
    [ 0,  0,  1],
    [ 1,  0,  1],
    [-1,  1,  1],
    [ 0,  1,  1],
    [ 1,  1,  1],
    [-1, -1, -1],
    [ 0, -1, -1],
    [ 1, -1, -1],
    [-1,  0, -1],
    [ 0,  0, -1],
    [ 1,  0, -1],
    [-1,  1, -1],
    [ 0,  1, -1],
    [ 1,  1, -1],
    [-1, -1,  0],
    [ 0, -1,  0],
    [ 1, -1,  0],
    [-1,  0,  0]
  ]

  for <<tryte::1-bytes <- @trytesAlphabet>> do
    {index, _} = :binary.match(@trytesAlphabet, tryte)
    trits = Enum.at(@trytesTrits, index)
    def trits?(unquote(tryte)) do
      unquote(trits)
    end
  end

  # convert trytes to integer

  @spec trytes_to_integer(binary) :: integer
  def trytes_to_integer(trytes) when is_binary(trytes) do
    trits_to_integer(trytes_to_trits(trytes))
  end

  # convert trits to integer

  @spec trits_to_integer(list) :: integer
  def trits_to_integer(trits) do
    trits_to_integer(trits, 0)
  end

  @spec trits_to_integer(list, integer) :: integer
  defp trits_to_integer([], returnvalue) do
    returnvalue
  end

  @spec trits_to_integer(list, integer) :: integer
  defp trits_to_integer([trit | trits], returnvalue) do
    returnvalue = returnvalue*3 + trit
    trits_to_integer(trits, returnvalue)
  end

  # convert trytes to trits

  @spec trytes_to_trits(binary) :: list
  def trytes_to_trits(trytes) when is_binary(trytes) do
    trytes_to_trits(trytes, [])
  end

  @spec trytes_to_trits(binary,list) :: list
  defp trytes_to_trits(<<tryte::1-bytes, trytes::binary>>, acc) do
    acc = revrese_acc(trits?(tryte), acc)
    trytes_to_trits(trytes, acc)
  end

  @spec trytes_to_trits(binary,list) :: list
  defp trytes_to_trits(<<>>, acc) do
    acc
  end

  @spec revrese_acc(list,list) :: list
  defp revrese_acc([h | t], acc) do
    revrese_acc(t, [h | acc])
  end

  @spec revrese_acc(list,list) :: list
  defp revrese_acc([], acc) do
    acc
  end

  @doc """
    convert dmp file line to tx_object.
  """
  @spec line_to_tx_object(binary) :: Transaction.t
  def line_to_tx_object(line) do
    # pattern matching
    <<hash::81-bytes,_,signature::2187-bytes,address::81-bytes,value::27-bytes,
    obsolete_tag::27-bytes,timestamp::9-bytes,current_index::9-bytes,
    last_index::9-bytes,bundle_hash::81-bytes, trunk::81-bytes,
    branch::81-bytes,tag::27-bytes, atime::9-bytes, alower::9-bytes,
    aupper::9-bytes, nonce::27-bytes,_,snapshot_index::binary>> = line
    # - First we convert some columns to varinteger form.
    last_index = trytes_to_integer(last_index)
    current_index = trytes_to_integer(current_index)
    value = trytes_to_integer(value)
    timestamp = trytes_to_integer(timestamp)
    atime = trytes_to_integer(atime)
    alower = trytes_to_integer(alower)
    aupper = trytes_to_integer(aupper)
    snapshot_index = String.to_integer(snapshot_index)
    # - Put transaction line in map.
    Transaction.create(signature, address,value, obsolete_tag,timestamp,
      current_index,last_index,bundle_hash,trunk,branch,tag,atime,
      alower,aupper,nonce,hash,snapshot_index)
  end

  @doc """
    Convert trytes to tx-object
  """
  @spec trytes_to_tx_object(binary, binary, integer) :: Transaction.t
  def trytes_to_tx_object(hash, trytes, snapshot_index) do
    <<signature::2187-bytes,address::81-bytes,value::27-bytes,
    obsolete_tag::27-bytes,timestamp::9-bytes,current_index::9-bytes,
    last_index::9-bytes,bundle_hash::81-bytes, trunk::81-bytes,
    branch::81-bytes,tag::27-bytes, atime::9-bytes, alower::9-bytes,
    aupper::9-bytes, nonce::27-bytes>> = trytes
    # - First we convert some columns to varinteger form.
    last_index = trytes_to_integer(last_index)
    current_index = trytes_to_integer(current_index)
    value = trytes_to_integer(value)
    timestamp = trytes_to_integer(timestamp)
    atime = trytes_to_integer(atime)
    alower = trytes_to_integer(alower)
    aupper = trytes_to_integer(aupper)
    # - Put transaction line in map.
    Transaction.create(signature, address,value, obsolete_tag,timestamp,
      current_index,last_index,bundle_hash,trunk,branch,tag,atime,
      alower,aupper,nonce,hash,snapshot_index)
  end

end
