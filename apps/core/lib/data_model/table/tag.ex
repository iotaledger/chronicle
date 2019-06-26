defmodule Core.DataModel.Table.Tag do

  @moduledoc """

    This is a CQL table schema for Tag,
    Please Check The following link for more details about DataTypes
    https://docs.scylladb.com/getting-started/types/

    :tag is the table name,
    :tg is a tag alias, and also the partition key,
    :ts is a timestamp alias, and also the first clustering key.
    :th is an txhash alias, also the second clustering key,

  """

  @default_ttl Application.get_env(:over_db, :core)[:__TAG_TTL__] || 1000 # seconds

  use OverDB.Builder.Table,
    keyspaces: [
      {Core.DataModel.Keyspace.Tangle, [:core]}
    ]

  table :tag do
    column :tg, :blob
    column :ts, :varint
    column :th, :blob
    partition_key [:tg]
    cluster_columns [:ts, :th]
    with_options [
    clustering_order_by: [
      ts: :desc
    ],
    default_time_to_live: @default_ttl
  ]
  end

end
