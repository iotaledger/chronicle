defmodule Broker.Collector.TxCollector.Helper do

  @moduledoc """
    This module hold the validating/decoding functions,
    it receives a tx_trytes from a feeder and do the required steps:
    - validate the hash of a transaction if it doesn't trust IRI flow.
    - it creates a tx-object from the tx_trytes.

  """
  alias Core.Utils.{Converter,Struct.Transaction}

  @spec create_tx_object(binary,binary, nil | integer) :: Transaction.t
  def create_tx_object(hash, tx_trytes, snapshot_index \\ nil) do
    Converter.trytes_to_tx_object(hash, tx_trytes, snapshot_index)
  end

end
