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

Additionally, you need to tweak Metabase `deps.edn` a bit.

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

Modifying `~/.clojure/deps.edn` will create two useful profiles: `user/clickhouse` that adds driver's sources to the classpath, and `user/test` that includes all the Metabase tests that are guaranteed to work with the driver.

* Install the Metabase dependencies:

```bash
clojure -X:deps:drivers prep
```

* Build the frontend:

```bash
yarn && yarn build-static-viz
```

* Start ClickHouse as a Docker container

```bash
docker compose -f modules/drivers/clickhouse/docker-compose.yml up -d clickhouse
```

Now, you should be able to run the tests:

```bash
DRIVERS=clickhouse clojure -X:dev:drivers:drivers-dev:test:user/clickhouse:user/test
```

you can see that we have our profiles `:user/clickhouse:user/test` added to the command above, and with `DRIVERS=clickhouse` we instruct Metabase to run the tests only for ClickHouse.

NB: Omitting `DRIVERS` will run the tests for all the built-in database drivers.

If you want to run tests for only a specific namespace:

```bash
DRIVERS=clickhouse clojure -X:dev:drivers:drivers-dev:test:user/clickhouse:user/test :only metabase.driver.clickhouse-test
```

or even a single test:

```bash
DRIVERS=clickhouse clojure -X:dev:drivers:drivers-dev:test:user/clickhouse:user/test :only metabase.driver.clickhouse-test/clickhouse-nullable-arrays
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
