# This file is responsible for configuring your application
# and its dependencies with the aid of the Mix.Config module.
use Mix.Config

# This configuration is loaded before any dependency and is restricted
# to this project. If another project depends on this project, this
# file won't be loaded nor affect the parent project. For this reason,
# if you want to provide default values for your application for
# third-party users, it should be done in your "mix.exs" file.

# NOTE: Here you can configure your broker application:
##########################################################
config :broker,
  __TOPICS__: [
    tx_trytes: [
      # add the zmq nodes urls to listen to tx_trytes(unconfirmed transactions) topic
      {'zmq.iota.org',5556}
    ],
    sn_trytes: [
      # add the zmq nodes urls to listen to sn_trytes(confirmed transactions) topic
    ]
  ],
  __MAX_DEMAND__: 64,
  __TX_TTL__: 10000, # Time to live ms (10 seconds)
  __BUNDLE_TTL__: 10000, # ms (10 seconds)
  __TRANSACTION_PARTITIONS_PER_TOPIC__: 2, # concurrent transactions validators/collector per topic(tx_trytes, sn_trytes)
  __BUNDLE_PARTITIONS_PER_TOPIC__: 2 # concurrent bundle validators/collectors per topic(tx_trytes, sn_trytes)

#
# and access this configuration in your application as:
#
#     Application.get_env(:broker, :key)
#
# You can also configure a third-party app:
#
#     config :logger, level: :info
#

# It is also possible to import configuration files, relative to this
# directory. For example, you can emulate configuration per environment
# by uncommenting the line below and defining dev.exs, test.exs and such.
# Configuration from the imported file will override the ones defined
# here (which is why it is important to import them last).
#
#     import_config "#{Mix.env()}.exs"
