<p align="center" style="font-size:300%">
<img src="https://www.metabase.com/images/logo.svg" width="200px" align="center">
<img src=".static/clickhouse.svg" width="180px" align="center">
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
<img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg">
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

For example [(using Metabase v0.47.2 and ClickHouse driver 1.2.2)](#choosing-the-right-version):

```bash
export METABASE_VERSION=v0.47.2
export METABASE_CLICKHOUSE_DRIVER_VERSION=1.2.2

mkdir -p mb/plugins && cd mb
curl -o metabase.jar https://downloads.metabase.com/$METABASE_VERSION/metabase.jar
curl -L -o plugins/ch.jar https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/$METABASE_CLICKHOUSE_DRIVER_VERSION/clickhouse.metabase-driver.jar
MB_PLUGINS_DIR=./plugins; java -jar metabase.jar
```

### Run as a Docker container

Alternatively, if you don't want to run Metabase Jar, you can use a Docker image:

```bash
export METABASE_DOCKER_VERSION=v0.47.2
export METABASE_CLICKHOUSE_DRIVER_VERSION=1.2.2

mkdir -p mb/plugins && cd mb
curl -L -o plugins/ch.jar https://github.com/ClickHouse/metabase-clickhouse-driver/releases/download/$METABASE_CLICKHOUSE_DRIVER_VERSION/clickhouse.metabase-driver.jar
docker run -d -p 3000:3000 \
  --mount type=bind,source=$PWD/plugins/ch.jar,destination=/plugins/clickhouse.jar \
  metabase/metabase:$METABASE_DOCKER_VERSION
```

## Choosing the Right Version

| Metabase Release | Driver Version |
| ---------------- | -------------- |
| 0.33.x           | 0.6            |
| 0.34.x           | 0.7.0          |
| 0.35.x           | 0.7.1          |
| 0.37.3           | 0.7.3          |
| 0.38.1+          | 0.7.5          |
| 0.41.2           | 0.8.0          |
| 0.41.3.1         | 0.8.1          |
| 0.42.x           | 0.8.1          |
| 0.44.x           | 0.9.1          |
| 0.45.x           | 1.1.0          |
| 0.46.x           | 1.1.7          |
| 0.47.x           | 1.2.3          |
| 0.47.7+          | 1.2.5          |
| 0.48.x           | 1.3.4          |
| 0.49.x           | 1.4.0          |

## Creating a Metabase Docker image with ClickHouse driver

You can use a convenience script `build_docker_image.sh`, which takes three arguments: Metabase version, ClickHouse driver version, and the desired final Docker image tag.

```bash
./build_docker_image.sh v0.47.2 1.2.2 my-metabase-with-clickhouse:v0.0.1
```

where `v0.47.2` is Metabase version, `1.2.2` is ClickHouse driver version, and `my-metabase-with-clickhouse:v0.0.1` being the tag.

Then you should be able to run it:

```bash
docker run -d -p 3000:3000 --name my-metabase my-metabase-with-clickhouse:v0.0.1
```

or use it with Docker compose, for example:

```yaml
version: '3.8'
services:
  clickhouse:
    image: 'clickhouse/clickhouse-server:23.8-alpine'
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

## Using certificates

In the "Advanced options", add the following to the "Additional JDBC connection string options" input:

```
sslrootcert=/path/to/ca.crt
```

where `/path/to/ca.crt` is the absolute path to the server CA on the Metabase host or Docker container (depends on your deployment).

Make sure that you tick "Use a secure connection (SSL)" as well.

## Operations

The driver should work fine for many use cases. Please consider the following items when running a Metabase instance with this driver:

* Create a dedicated user for Metabase, whose profile has `readonly` set to 2.
* Consider running the Metabase instance in the same time zone as your ClickHouse database; the more time zones involved the more issues.
* Compare the results of the queries with the results returned by `clickhouse-client`.
* Metabase is a good tool for organizing questions, dashboards etc. and to give non-technical users a good way to explore the data and share their results. The driver cannot support all the cool special features of ClickHouse, e.g. array functions. You are free to use native queries, of course.


## Known limitations

* As the underlying JDBC driver version does not support columns with `AggregateFunction` type, these columns are excluded from the table metadata and data browser result sets to prevent sync or data browsing errors.
* If the past month/week/quarter/year filter over a DateTime64 column is not working as intended, this is likely due to a [type conversion issue](https://github.com/ClickHouse/ClickHouse/pull/50280). See [this report](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/164) for more details. This issue was resolved as of ClickHouse 23.5.
* If introspected ClickHouse version is lower than 23.8, the driver will not use [startsWithUTF8](https://clickhouse.com/docs/en/sql-reference/functions/string-functions#startswithutf8) and fall back to its [non-UTF8 counterpart](https://clickhouse.com/docs/en/sql-reference/functions/string-functions#startswith) instead. There is a drawback in this compatibility mode: potentially incorrect filtering results when working with non-latin strings. If your use case includes filtering by columns with such strings and you experience these issues, consider upgrading your ClickHouse server to 23.8+.

## Contributing

Check out our [contributing guide](./CONTRIBUTING.md).
