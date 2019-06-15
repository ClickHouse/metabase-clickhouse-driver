# metabase-clickhouse-driver

[MetaBase](https://metabase.com) ([github](https://github.com/metabase/metabase)) driver for the [ClickHouse](https://clickhouse.yandex) database ([github](https://github.com/yandex/ClickHouse))

![OnTime table in MetaBase](docs/images/metabase_clickhouse_ontime_teaser.png)

[![CircleCI](https://circleci.com/gh/enqueue/metabase-clickhouse-driver.svg?style=svg)](https://circleci.com/gh/enqueue/metabase-clickhouse-driver)

# Testing

:warning: _This driver is in an early development stage and we would caution against using it in your production environment_ :warning:

## Download MetaBase Jar and Run

1. Download a fairly recent MetaBase binary release (jar file) from the [MetaBase distribution page](https://metabase.com/start/jar.html).
2. Download the ClickHouse driver jar from this repository's "Releases" page
3. Create a directory and copy the `metabase.jar` to it.
4. In that directory create a sub-directory called `plugins`.
5. Copy the ClickHouse driver jar to the `plugins` directory.
6. Make sure you are the in the directory where your `metabase.jar` lives.
7. Run `MB_PLUGINS_DIR=./plugins; java -jar metabase.jar`.

## Building from Source

Please refer to the extensive documentation available in the MetaBase Wiki: [Writing A Driver](https://github.com/metabase/metabase/wiki/Writing-A-Driver)

1. Clone [metabase repository](https://github.com/metabase/metabase)
2. Clone this repository, and follow [the instructions for building the driver](https://github.com/metabase/metabase/wiki/Writing-a-Driver:-Packaging-a-Driver-&-Metabase-Plugin-Basics). Alternatively, download a pre-release jar.
3. Copy `clickhouse.metabase-driver.jar` into your MetaBase `plugins` directory
4. Start MetaBase, e.g. by invoking `lein run` in your MetaBase directory.

## Do the Docker Dance

In an empty directory, create your Dockerfile, e.g. `Dockerfile-clickhouse`

```
FROM metabase/metabase-head:latest
ADD https://github.com/enqueue/metabase-clickhouse-driver/releases/download/v0.1/clickhouse.metabase-driver.jar /app/plugins/
```

Assemble

```
docker build -f Dockerfile-clickhouse -t foo/metabase-with-clickhouse .
```

Run

```
docker run --rm -d=false -p 3000:3000 --name metabase foo/metabase-with-clickhouse
```

Please refer to [the fine MetaBase operations manual](https://www.metabase.com/docs/latest/operations-guide/running-metabase-on-docker.html) to find out how to operate a dockerized MetaBase with a regular database.


# Contributing
* Report any issues you encounter
* Just create a pull request, preferably with a test or five

# History
The request for a ClickHouse MetaBase driver is formulated in [MetaBase issue #3332](https://github.com/metabase/metabase/issues/3332). Some impatient ClickHouse users started development. The MetaBase team is asking driver developers to publish plug-ins and collect some experiences before considering a PR, so here we are.

This driver is based on the following PRs:
* [metabase#8491](https://github.com/metabase/metabase/pull/8491)
* [metabase#8722](https://github.com/metabase/metabase/pull/8722)
* [metabase#9469](https://github.com/metabase/metabase/pull/9469)

# Contributors, Authors
The initial source base comprises major contributions from these authors (_the git log has suffered from frequent brutal rebases, please add yourself here, if I missed you!_):

* Bogdan Mukvich (@Badya)
* @tsl-karlp
* Andrew Grigorev (@ei-grad)
* Felix Mueller (@enqueue)
