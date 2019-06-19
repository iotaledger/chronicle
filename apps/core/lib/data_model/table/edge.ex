defmodule Core.DataModel.Table.Edge do

  @moduledoc """

    This is a CQL table schema for Edge,
    Please Check The following link for more details about DataTypes
    https://docs.scylladb.com/getting-started/types/

    :edge is the table name,
    :v1 is a vertex_one alias, and also the partition key,
    which act as distrbuited shard secondary index for the bundle
    relations, for now it only hold outputs/inputs/tx_hashes/tips.
    :lb is a label alias, and also the first clustering key.
    :ts is a bundle_timestamp alias, and also the second clustering key.
    :v2 is an vertex_two alias, also the third clustering key,
    and hold the bundle_hash.
    :ex is an extra_vertex alias, also the 4th clustering key,
    and hold the address's value(trytes form) when the row's label
    is input/output and the head_hash when the labels are
    tx_hash/approve/head_hash.
    :ix is an index alias, also the 5th clustering key,
    which hold the current_index(signed-varint form)

    we consider the transaction is an input if the following cond is met:
      - value of the transaction is less than zero.

    we consider the transaction is an output if the following cond is met:
      - value of the transaction is equal or greater than zero.

    example: tx_hash for an input at current_index 5 will be stored as -5.

    if the label is approve,then the indexes are only (0 or 1) where
     0 indicates trunk_tip(tip0)
     1 indicates branch_tip(tip1)

    :el is an extra label alias, and non-clustering-column,
    it hold an extra label in varint form, mostly it's used for tx-hash
    rows, it will indicate whether the tx-hash row belongs to input or output.

    # NOTE: the labels are stored in tinyint form, %{

      10 => :output,
      20 => :input,
      30 => :txhash,
      40 => :headHash,
      50 => :approve

    }

    :lx is a last_index alias, and non-clustering-column,
    it hold the same value(last_index in varint form) no matter
    what the labels are.

    :sx is a snapshot_index alias, and non-clustering-column,
    it hold the same value(snapshot_index in trytes form),
    keep in mind the intitial state of snapshot_index is NULL
    for any bundle (tx-objects siblings in a given attachment)
    this field should be inserted only for confirmed bundles,
    never insert snapshot_index for un-confirmed bundles, at
    anycost, as it might overwrite the confirmed status.

  """

  use OverDB.Builder.Table,
    keyspaces: [
      {Core.DataModel.Keyspace.Tangle, [:core]}
    ]

  table :edge do
    column :v1, :blob
    column :lb, :tinyint
    column :ts, :varint
    column :v2, :blob
    column :ex, :blob
    column :ix, :varint
    column :el, :varint
    column :lx, :varint
    column :sx, :varint
    partition_key [:v1]
    cluster_columns [:lb, :ts, :v2, :ex, :ix]
    with_options [
    clustering_order_by: [
      lb: :asc,
      ts: :desc
    ]
  ]
  end

end
