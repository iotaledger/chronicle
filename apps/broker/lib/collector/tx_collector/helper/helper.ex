defmodule Broker.Collector.TxCollector.Helper do

  @moduledoc """
    This module hold the validating/decoding functions,
    it receives a tx_trytes from a feeder and do the required steps:
    - validate the hash of a transaction if it doesn't trust IRI flow.
    - it creates a tx-object from the tx_trytes.

  """
  @verify Application.get_env(:broker, :verify) || true
  alias Core.Utils.{Converter,Struct.Transaction}

  if @verify do
    @spec verify_hash(binary, binary) :: boolean
    def verify_hash(_hash,_tx_trytes) do
      # verifying logic for now we will put true
      true
    end
  else
    @spec verify_hash(binary, binary) :: boolean
    def verify_hash(_hash,_tx_trytes) do
      true
    end
  end

  @spec create_tx_object(binary,binary) :: Transaction.t
  def create_tx_object(hash, tx_trytes) do
    Converter.trytes_to_tx_object(hash, tx_trytes)
  end

end
