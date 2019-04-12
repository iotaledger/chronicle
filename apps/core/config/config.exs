# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# third-party users, it should be done in your "mix.exs" file.

# You can configure your application as:
#
#     config :core, key: :value
#
# and access this configuration in your application as:
#
#     Application.get_env(:core, :key)
#
# You can also configure a third-party app:
#
#     config :logger, level: :info
#
      config :over_db, :core,
        __USERNAME__: "username", # CQL username for auth
        __PASSWORD__: "password", # CQL password for auth
        __DATA_CENTERS__: [
          dc1: [
            {'127.0.0.1', 9042},
          ],
        ],
        __RING__: :core_ring,
        __RECEIVER_PRIORITY__: :high,
        __REPORTERS_PER_SHARD__: 2,
        __CONNS_PER_SHARD__: 2,
        __LOGGED_PER_SHARD__: 1,
        __UNLOGGED_PER_SHARD__: 1,
        __COUNTER_PER_SHARD__: 1

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
     import_config "*/config.exs" 
