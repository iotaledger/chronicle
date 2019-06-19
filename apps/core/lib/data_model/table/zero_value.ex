defmodule Core.DataModel.Table.ZeroValue do

  @moduledoc """

    This is a CQL table schema for ZeroValue,
    Please Check The following link for more details about DataTypes
    https://docs.scylladb.com/getting-started/types/
    it's also important to mention this table is meant to store the
    address's relations for zero_value_bundles. and it consumes
    two labels (:output, :input).

    :zero_value is the table name,
    :v1 is a vertex_one alias, and also the first component_partition_key,
    :yy is a year alias, and also the second component_partition_key,
    :mm is a month alias, and the last component_partition_key, thus
    the complete composite partition_key is (:v1, :yy, :mm)
    :lb is a label alias, and also the first clustering key.
    :ts is a bundle_timestamp alias, and also the second clustering key.
    :v2 is an vertex_two alias, also the third clustering key,
    and hold the bundle_hash.
    :ix is an index alias, also the 4th clustering key,
    which hold the current_index(signed-varint form)

    # NOTE: the consumed labels are stored in tinyint form, %{

      10 => :output,
      20 => :input,

    }

    :lx is a last_index alias, and non-clustering-column,
    it hold the same value(last_index in varint form) no matter
    what the labels are.

  """

  use OverDB.Builder.Table,
    keyspaces: [
      {Core.DataModel.Keyspace.Tangle, [:core]}
    ]

  table :zero_value do
    column :v1, :blob
    column :yy, :smallint
    column :mm, :smallint
    column :lb, :tinyint
    column :ts, :varint
    column :v2, :blob
    column :ex, :blob
    column :ix, :varint
    column :el, :varint
    column :lx, :varint
    column :sx, :varint
    partition_key [:v1, :yy, :mm]
    cluster_columns [:lb, :ts, :v2, :ex, :ix]
    with_options [
    clustering_order_by: [
      lb: :asc,
      ts: :desc
    ]
  ]
  end

end
