# Chronicle

Chronicle is a distributed fault tolerance permanode that scales out and up, designed and developed in a battle tested Elixir/Erlang ecosystem and will be ported to Rust. It provides a complex network infrastructure which can be extended with various functionality through Multiplex networks, by building microservices for each layer, that can communicate with public and private dataset(s) under different policies.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for testing purposes.

### Prerequisites

What things you need to install and have in advance

- Independent running Scylla cluster ([ScyllaDB](https://docs.scylladb.com/getting-started/) >= 3.0.6)
- [Elixir](https://elixir-lang.org/install.html) >= 1.8.1 and [Phoenix](https://hexdocs.pm/phoenix/installation.html) up and running in Chronicle node
- Installed [Bazel](https://docs.bazel.build/versions/master/install.html)



### Installing

A step by step series of commands that tell you how to get a chronicle up and running

**Step 1 Configuration**
- Clone the repo `git clone https://github.com/iotaledger/chronicle.git`
- Open Chronicle folder using a source code editor (ie [Atom](https://atom.io/))
- Configure the Core app in the following config file `apps/core/config/config.exs`
- Configure the Broker app in the following config file `apps/broker/config/config.exs`
- Download all the [dmps](https://u204324.your-storagebox.de/data/)
- Put the dmps in path/to/chronicle/historical/data
- Make sure path/to/chronicle/historical/dmps.txt has all the filenames line by line (don't forget to close it)

**Step 2 Commands**

- Fetch the required dependencies
    - `mix deps.get`
- Generate phoenix secret and then copy the 64-bytes secret string
    - `mix phx.gen.secret`
- Compile the project
    - `mix deps.compile`
    - `mix compile`
- Start/Run Chronicle

    - `SECRET_KEY_BASE=thegeneratedsecretfromabovecommand PORT=4000 HOST=localhost MIX_ENV=prod elixir --name app@hostname --cookie "MyChronicleCookie" -S mix run --no-halt`

- Enjoy

NOTE: Chronicle will start importing the dmps files into ScyllaDB the moment it boots.


## The supported API calls

- **getTrytes**
```
curl http://host:port/api \
-X POST \
-H 'Content-Type: application/json' \
-H 'X-IOTA-API-Version: 1' \
-d '{
"command": "getTrytes",
"hashes": [
  "TRANSACTION_HASH_1","TRANSACTION_HASH_N"
  ]
}'
```
- **findTransactions**
```
curl http://host:port/api \
-X POST \
-H 'Content-Type: application/json' \
-H 'X-IOTA-API-Version: 1' \
-d '{
"command": "findTransactions",
"addresses": [
  "ADDRESS_1","ADDRESS_N"
  ]
}'
```
- **findTransactions**
```
curl http://host:port/api \
-X POST \
-H 'Content-Type: application/json' \
-H 'X-IOTA-API-Version: 1' \
-d '{
"command": "findTransactions",
"bundles": [
  "BUNDLE_HASH_1", "BUNDLE_HASH_N"
  ]
}'
```
- **findTransactions**
```
curl http://host:port/api \
-X POST \
-H 'Content-Type: application/json' \
-H 'X-IOTA-API-Version: 1' \
-d '{
"command": "findTransactions",
"approvees": [
  "HASH_1", "HASH_N"
  ]
}'
```
- **findTransactions** by tags
Note: the expected response is tags hints
```
curl http://host:port/api \
-X POST \
-H 'Content-Type: application/json' \
-H 'X-IOTA-API-Version: 1' \
-d '{
"command": "findTransactions",
"tags": [
  "TAG_1", "TAG_N"
  ]
}'
```
- **findTransactions** by tag hint
Note: the minimum required tag length is 4-trytes to search by IAC(IOTA AREA CODE)
```
curl http://host:port/api \
-X POST \
-H 'Content-Type: application/json' \
-H 'X-IOTA-API-Version: 1' \
-d '{
"command": "findTransactions",
"hints": [
  {"tag":"999999999999999999999999999","month":8,"year":2019, "page_size": 500}
  ]
}'
```
- **findTransactions** by address hint
```
curl http://host:port/api \
-X POST \
-H 'Content-Type: application/json' \
-H 'X-IOTA-API-Version: 1' \
-d '{
"command": "findTransactions",
"hints": [
  {"address":"JPYUAV9MBDZG9ZX9BAPBBMYFEVORNBIOOZCYPZDZNRGKQYT9HFEXXXBG9TULULJIOWJWQMXSPLILOJGJG","month":8,"year":2019, "page_size": 500}
  ]
}'
```

## Deployment

This project is not intended for production use, as we are currently working on a Rust version for the final release, and that is our highest priority.

## Built With

* [Elixir](https://elixir-lang.org/) - The main language
* [Phoenix](https://phoenixframework.org/) - The web framework

## Contributing

Please read [CONTRIBUTING.md]()


## Authors

* **Louay Kamel** - *Initial work*

See also the list of [contributors](https://github.com/iotaledger/chronicle/graphs/contributors) who participated in this project.

## License

This project is licensed under the Apache 2 License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Thanks to anyone whose library was used
