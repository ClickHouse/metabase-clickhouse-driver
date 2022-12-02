<p align="center" style="font-size:300%">
<img src="https://www.metabase.com/images/logo.svg" width="200px" align="center">
<img src=".static/logo.png" width="200px" align="center">
<h1 align="center">ClickHouse driver for Metabase</h1>
</p>
<br/>
<p align="center">
<a href="https://github.com/enqueue/metabase-clickhouse-driver/actions/workflows/check.yml">
<img src="https://github.com/enqueue/metabase-clickhouse-driver/actions/workflows/check.yml/badge.svg?branch=master">
</a>
<a href="https://github.com/enqueue/metabase-clickhouse-driver/releases">
<img src="https://img.shields.io/github/release/enqueue/metabase-clickhouse-driver.svg?label=latest%20release">
</a>
<a href="https://raw.githubusercontent.com/enqueue/metabase-clickhouse-driver/master/LICENSE">
<img src="https://img.shields.io/badge/license-AGPL-05B8CC.svg">
</a>
</p>

## About

[ClickHouse](https://clickhouse.com) ([GitHub](https://github.com/ClickHouse/ClickHouse)) database driver for the [Metabase](https://metabase.com) ([GitHub](https://github.com/metabase/metabase)) business intelligence front-end.

## Installation

### Run using Metabase Jar

1. Download a fairly recent Metabase binary release (jar file) from the [Metabase distribution page](https://metabase.com/start/jar.html).
2. Download the ClickHouse driver jar from this repository's [Releases](https://github.com/enqueue/metabase-clickhouse-driver/releases) page
3. Create a directory and copy the `metabase.jar` to it.
4. In that directory create a sub-directory called `plugins`.
5. Copy the ClickHouse driver jar to the `plugins` directory.
6. Make sure you are the in the directory where your `metabase.jar` lives.
7. Run `MB_PLUGINS_DIR=./plugins; java -jar metabase.jar`.

For example (using Metabase v0.44.6 and ClickHouse driver 0.8.3):

```bash
mkdir -p mb/plugins && cd mb
curl -o metabase.jar https://downloads.metabase.com/v0.44.6/metabase.jar
curl -L -o plugins/ch.jar https://github.com/enqueue/metabase-clickhouse-driver/releases/download/0.8.3/clickhouse.metabase-driver.jar
MB_PLUGINS_DIR=./plugins; java -jar metabase.jar
```

### Run as a Docker container

Alternatively, if you don't want to run Metabase Jar, you can use a Docker image:

```bash
mkdir -p mb/plugins && cd mb
curl -L -o plugins/ch.jar https://github.com/enqueue/metabase-clickhouse-driver/releases/download/0.8.3/clickhouse.metabase-driver.jar
docker run -d -p 3000:3000 \
  --mount type=bind,source=$PWD/plugins/ch.jar,destination=/plugins/clickhouse.jar \
  --name metabase metabase/metabase:v0.44.6
```

## Choosing the Right Version

Metabase Release | Driver Version
---------------- | --------------
0.33.x           | 0.6
0.34.x           | 0.7.0
0.35.x           | 0.7.1
0.37.3           | 0.7.3
0.38.1+          | 0.7.5
0.41.2           | 0.8.0
0.41.3.1         | 0.8.1
0.42.x           | 0.8.1
0.44.x           | 0.8.3

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
docker compose -f modules/drivers/clickhouse/docker-compose.yml up -d
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

## Creating a Metabase Docker image with ClickHouse driver

You can use a convenience script `build_docker_image.sh` which takes three arguments: Metabase version, ClickHouse driver version, and the desired final Docker image tag.

```bash
./build_docker_image.sh v0.44.6 0.8.3 my-metabase-with-clickhouse:v0.0.1
```

where `v0.44.6` is Metabase version, `0.8.3` is ClickHouse driver version, and `my-metabase-with-clickhouse:v0.0.1` being the tag.

Then you should be able to run it:

```bash
docker run -d -p 3000:3000 --name my-metabase my-metabase-with-clickhouse:v0.0.1
```

or use with Docker compose, for example:

```yaml
version: '3.8'
services:
  clickhouse:
    image: 'clickhouse/clickhouse-server:22.10.2-alpine'
    container_name: 'metabase-clickhouse-server'
    ports:
      - '8123:8123'
      - '9000:9000'
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
  metabase:
    image: 'my-metabase-with-clickhouse:v0.0.1'
    container_name: 'metabase-with-clickhouse'
    ports:
      - '3000:3000'
```

## Operations

The driver should work fine for many use cases. Please consider the following items when running a Metabase instance with this driver:

* Create a dedicated user for Metabase, whose profile has `readonly` set to 2.
* Consider running the Metabase instance in the same time zone as your ClickHouse database; the more time zones involved the more issues.
* Compare the results of the queries with the results returned by `clickhouse-client`.
* Metabase is a good tool for organizing questions, dashboards etc. and to give non-technical users a good way to explore the data and share their results. The driver cannot support all the cool special features of ClickHouse, e.g. array functions. You are free to use native queries, of course.


## Contributing

* Report any issues you encounter during operations
* Create a pull request, preferably with a test or five
* See the very useful documentation by the Metabase team: [Writing A Driver](https://github.com/metabase/metabase/wiki/Writing-A-Driver)

## License

The contents of this repository are made available under the GNU Affero General Public License v3.0 (AGPL), see [LICENSE](https://github.com/enqueueu/metabase-clickhouse-driver/blob/master/LICENSE).

Unless explicitly stated differently in the respective file, all files are `Copyright 2018-2022 the metabase-clickhouse-driver contributors`.
