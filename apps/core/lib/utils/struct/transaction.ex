defmodule Core.Utils.Struct.Transaction do

  defstruct [
    :attachment_timestamp_upper,
    :attachment_timestamp_lower,
    :attachment_timestamp,
    :signature_or_message,
    :snapshot_index,
    :obsolete_tag,
    :current_index,
    :last_index,
    :timestamp,
    :address,
    :bundle,
    :value,
    :hash,
    :trunk,
    :branch,
    :nonce,
    :tag
  ]

  @type int_or_bin :: integer | binary
  @type t :: %__MODULE__{signature_or_message: term,
   address: binary,
   value: int_or_bin,  # integer type
   obsolete_tag: binary,
   timestamp: int_or_bin,  # integer type in seconds
   current_index: int_or_bin,
   last_index: int_or_bin, # integer type
   bundle: binary,
   trunk: binary,
   branch: binary,
   tag: binary,
   attachment_timestamp: int_or_bin,  # integer type in milliseconds
   attachment_timestamp_lower: int_or_bin,  # integer type
   attachment_timestamp_upper: int_or_bin,  # integer type
   nonce: binary,
   hash: binary,
   snapshot_index: integer  }


  def create(signature, address,value, obsolete_tag,timestamp,
    current_index,last_index,bundle_hash,trunk,branch,tag,atime,
    alower,aupper,nonce,hash,snapshot_index \\ nil) do
    %__MODULE__{signature_or_message: signature,
     address: address,
     value: value,  # integer type
     obsolete_tag: obsolete_tag,
     timestamp: timestamp,  # integer type in seconds
     current_index: current_index,
     last_index: last_index, # integer type
     bundle: bundle_hash,
     trunk: trunk,
     branch: branch,
     tag: tag,
     attachment_timestamp: atime,  # integer type in milliseconds
     attachment_timestamp_lower: alower,  # integer type
     attachment_timestamp_upper: aupper,  # integer type
     nonce: nonce,
     hash: hash,
     snapshot_index: snapshot_index  # nil or integer type
     }
  end


end
