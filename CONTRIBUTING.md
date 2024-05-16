## Getting started

ClickHouse driver for Metabase is an open-source project,
and we welcome any contributions from the community.
Please share your ideas, contribute to the codebase,
and help us maintain up-to-date documentation.

* Please report any issues you encounter during operations.
* Feel free to create a pull request, preferably with a test or five.

## Setting up a development environment

### Requirements

* Clojure 1.11+
* OpenJDK 17
* Node.js 16.x
* Yarn

For testing: Docker Compose

Please refer to the extensive documentation available on the Metabase website: [Guide to writing a Metabase driver](https://www.metabase.com/docs/latest/developers-guide/drivers/start.html)

ClickHouse driver's code should be inside the main Metabase repository checkout in `modules/drivers/clickhouse` directory.

Additionally, you need to tweak Metabase's `deps.edn` a bit.

The easiest way to set up a development environment is as follows (mostly the same as in the [CI](https://github.com/enqueue/metabase-clickhouse-driver/blob/master/.github/workflows/check.yml)):

* Clone Metabase and ClickHouse driver repositories
```bash
git clone https://github.com/metabase/metabase.git
cd metabase
git clone https://github.com/enqueue/metabase-clickhouse-driver.git modules/drivers/clickhouse
```

* Create custom Clojure profiles

```bash
mkdir -p ~/.clojure
cat modules/drivers/clickhouse/.github/deps.edn | sed -e "s|PWD|$PWD|g" > ~/.clojure/deps.edn
```

Modifying `~/.clojure/deps.edn` will create a new profile: `user/clickhouse`, that adds driver's sources to the class path, and includes all the Metabase tests that are guaranteed to work with the driver.

* Install the Metabase dependencies:

```bash
clojure -X:deps:drivers prep
```

* Build the frontend:

```bash
yarn && yarn build-static-viz
```

* Add /etc/hosts entry

Required for TLS tests.

```bash
sudo -- sh -c "echo 127.0.0.1 server.clickhouseconnect.test >> /etc/hosts"
```

* Start Docker containers

```bash
docker compose -f modules/drivers/clickhouse/docker-compose.yml up -d
```

Here's an overview of the started containers, which have the ports exposed to the `localhost` (see [docker-compose.yml](./docker-compose.yml)):

- Metabase with the ClickHouse driver loaded from the JAR file (port: 3000)
- Current ClickHouse version (port: 8123) - the main instance for all tests.
- Current ClickHouse cluster with two nodes (+ nginx as an LB, port: 8127) - required for the set role tests (verifying that the role is set correctly via the query parameters).
- Current ClickHouse version with TLS support (port: 8443) - required for the TLS tests.
- Older ClickHouse version (port: 8124) - required for the string functions tests (switch between UTF8 (current) and non-UTF8 (pre-23.8) versions), as well as to verify that certain features, such as connection impersonation, are disabled on the older server versions.

Now, you should be able to run the tests:

```bash
DRIVERS=clickhouse clojure -X:dev:drivers:drivers-dev:test:user/clickhouse
```

you can see that we have our `:user/clickhouse` profile added to the command above, and with `DRIVERS=clickhouse` we instruct Metabase to run the tests only for ClickHouse.

NB: Omitting `DRIVERS` will run the tests for all the built-in database drivers.

If you want to run tests for only a specific namespace:

```bash
DRIVERS=clickhouse clojure -X:dev:drivers:drivers-dev:test:user/clickhouse :only metabase.driver.clickhouse-test
```

or even a single test:

```bash
DRIVERS=clickhouse clojure -X:dev:drivers:drivers-dev:test:user/clickhouse :only metabase.driver.clickhouse-test/clickhouse-nullable-arrays
```

Testing the driver with the older ClickHouse version (see [docker-compose.yml](./docker-compose.yml)):

```bash
MB_CLICKHOUSE_TEST_PORT=8124 DRIVERS=clickhouse clojure -X:dev:drivers:drivers-dev:test:user/clickhouse :only metabase.driver.clickhouse-test
```

## Building a jar

You need to add an entry for ClickHouse in `modules/drivers/deps.edn`

```clj
{:deps
 {...
  metabase/clickhouse {:local/root "clickhouse"}
  ...}}
```

or just run this from the root Metabase directory, overwriting the entire file:

```bash
echo "{:deps {metabase/clickhouse {:local/root \"clickhouse\" }}}" > modules/drivers/deps.edn
```

Now, you should be able to build the final jar:

```bash
bin/build-driver.sh clickhouse
```

As the result, `resources/modules/clickhouse.metabase-driver.jar` should be created.

For smoke testing, there is a Metabase with the link to the driver available as a Docker container:

```bash
docker compose -f modules/drivers/clickhouse/docker-compose.yml up -d metabase
```

It should pick up the driver jar as a volume.
