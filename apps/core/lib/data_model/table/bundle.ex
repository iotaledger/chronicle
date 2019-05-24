defmodule Core.DataModel.Table.Bundle do

  @moduledoc """

    This is a CQL table schema for bundle,
    Please Check The following link for more details about DataTypes
    https://docs.scylladb.com/getting-started/types/

    :bundle is the table name,
    :bh is a bundle_hash alias, and also the partition_key(pk),
    which mean the whole parition(the rows with same pk) stored in same shard.
    :lb is a label alias, and also the first clustering key.
    :ts is a bundle_timestamp alias, and also the second clustering key.
    :ix is an index alias, and also the third clustering key.
    :id is an id, and also the forth clustering key.
    :va is a value, and also the last clustering key,
      if labels(lb) are output or input, the :va hold the address trytes,
      if labels(lb) are txhash or headhash, the :va indicates whether it's
      input or output.

    all the next non-clustering-columns are named to form
    alphabetical/asc order, and the names don't hold anymeaning,
    because they depend on the row's label.
    # NOTE: lb clustering column hold the labels in tinyint form.
    the bundle table is consuming the following tinyint-labels %{

      10 => :output,
      20 => :input,
      30 => :txhash,
      40 => :headHash

    }

    :a column hold the address's value(trytes form) when the row's label/lb is :output or :input,
    and hold the snapshot_index(trytes form) when the the row's label is :txhash or :headHash.
    keep in mind we should not touch the snapshot_index for un-confirmed bundles,

    :b column hold lastIndex(trytes form) when the row's label is :output or :input,
    and hold the transaction hash(trytes form) when the row's label is :txHash or :headHash.

    :c column hold bundle_nonce(trytes form) when the row's label is :output or :input,
    and hold the transaction_nonce(trytes form) when the row's label is :txHash or :headHash.

    :d column hold obsoleteTag(trytes form) when the row's label is :output or :input,
    and hold the transaction_tag when the row's label is :txHash or :headHash.

    :e column hold signatureFragment(trytes form) when the row's label is :output or :input,
    and hold the trunk(trytes form) when the row's label is :txHash or :headHash.

    :f column hold nothing when the row's label is :output or :input,
    but hold the branch(trytes form) when the row's label is :txHash or :headHash.

    :g column hold nothing when the row's label is :output or :input,
    but hold the attachment_timestamp(trytes form) when the row's label is :txHash or :headHash.

    :h column hold nothing when the row's label is :output or :input,
    but hold the attachment_timestamp_upper_bound(trytes form) when the row's label is :txHash or :headHash.

    :i column hold nothing when the row's label is :output or :input,
    but hold the attachment_timestamp_lower_bound(trytes form) when the row's label is :txHash or :headHash.

  """

  use OverDB.Builder.Table,
    keyspaces: [
      {Core.DataModel.Keyspace.Tangle, [:core]}
    ]

  table :bundle do
    column :bh, :varchar
    column :lb, :tinyint
    column :ts, :varint
    column :ix, :varint
    column :id, :varchar
    column :va, :varchar
    column :a, :varchar
    column :b, :varchar
    column :c, :varchar
    column :d, :varchar
    column :e, :varchar
    column :f, :varchar
    column :g, :varchar
    column :h, :varchar
    column :i, :varchar
    partition_key [:bh]
    cluster_columns [:lb, :ts, :ix, :id, :va]
    with_options [
    clustering_order_by: [
      lb: :asc,
      ts: :desc
    ]
  ]
  end

end
