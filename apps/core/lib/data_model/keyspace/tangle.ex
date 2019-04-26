defmodule Core.DataModel.Keyspace.Tangle do

  @moduledoc """

    This is the tangle CQL Keyspace,
    A keyspace is a collection of tables with attributes which
    define how data is replicated on nodes, the current
    tables(bundle/edge) reside in this keyspace.

    # NOTE: Currently we only support SimpleStrategy,
    that is recommended for one datacenter setup.

    currently we are working on a better structure .
    
  """

  use OverDB.Builder.Keyspace,
    otp_apps: [:core]

  keyspace :tangle  do
    replication_factor 3
    with_options [
      replication: "{'class' : 'SimpleStrategy', 'replication_factor': 3}"
    ]
  end

end
