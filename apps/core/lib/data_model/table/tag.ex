defmodule Core.DataModel.Table.Tag do

  @moduledoc """

    This is a CQL table schema for Tag,
    Please Check The following link for more details about DataTypes
    https://docs.scylladb.com/getting-started/types/

    :tag is the table name,
    :p0, is pair0 alias first 2 chars of tag, in IAC they represent 2200km # first component in pk,
    :p1, is pair1 alias, second 2 chars from tag, in IAC they represent 110km # second component in pk
    :yy, is year alias, third component partition key
    :mm, is month alias, 4th component partition key
    :p2, is pair2 alias, third 2 chars from tag, in IAC they represent 5.5 km
    :p3, is pair3 alias, 4th 2 chars from tag, in IAC they represent 275m
    :rt is remaining tag alias, remaining 19 chars from tag.
    :ts is a timestamp alias, and also the 6th clustering key.
    :th is txhash alias, also the last clustering key,

  """

  @default_ttl Application.get_env(:over_db, :core)[:__TAG_TTL__] || 0 # seconds, 0 = forever.

  use OverDB.Builder.Table,
    keyspaces: [
      {Core.DataModel.Keyspace.Tangle, [:core]}
    ]

  table :tag do
    column :p0, :varchar
    column :p1, :varchar
    column :yy, :smallint
    column :mm, :smallint
    column :p2, :varchar
    column :p3, :varchar
    column :rt, :varchar
    column :ts, :varint
    column :th, :blob
    partition_key [:p0, :p1, :yy, :mm]
    cluster_columns [:p2, :p3, :rt, :ts, :th]
    with_options [
    clustering_order_by: [
      p2: :asc,
      p3: :asc,
      rt: :asc,
      ts: :desc
    ],
    default_time_to_live: @default_ttl
  ]
  end

end
