# metabase-clickhouse-driver

[ClickHouse](https://clickhouse.yandex) ([github](https://github.com/ClickHouse/ClickHouse)) database driver for the [Metabase](https://metabase.com) ([github](https://github.com/metabase/metabase)) business intelligence front-end

![OnTime table in Metabase](docs/images/metabase_clickhouse_ontime_teaser.png)

[![CircleCI](https://circleci.com/gh/enqueue/metabase-clickhouse-driver.svg?style=svg)](https://circleci.com/gh/enqueue/metabase-clickhouse-driver)
[![Latest Release](https://img.shields.io/github/release/enqueue/metabase-clickhouse-driver.svg?label=latest%20release)](https://github.com/enqueue/metabase-clickhouse-driver/releases)
[![GitHub license](https://img.shields.io/badge/license-AGPL-05B8CC.svg)](https://raw.githubusercontent.com/enqueue/metabase-clickhouse-driver/master/LICENSE.txt)

# Warning :construction:

The current driver version is incompatible with Metabase 0.34 (see issue [#43](../../issues/43)). I am trying to make it work again. Please create a PR if you have a fix ready.

# Installation

## Download Metabase Jar and Run

1. Download a fairly recent Metabase binary release (jar file) from the [Metabase distribution page](https://metabase.com/start/jar.html).
2. Download the ClickHouse driver jar from this repository's "Releases" page
3. Create a directory and copy the `metabase.jar` to it.
4. In that directory create a sub-directory called `plugins`.
5. Copy the ClickHouse driver jar to the `plugins` directory.
6. Make sure you are the in the directory where your `metabase.jar` lives.
7. Run `MB_PLUGINS_DIR=./plugins; java -jar metabase.jar`.

## Building from Source

Please refer to the extensive documentation available in the Metabase Wiki: [Writing A Driver](https://github.com/metabase/metabase/wiki/Writing-A-Driver)

1. Clone [metabase repository](https://github.com/metabase/metabase)
2. Clone this repository, and follow [the instructions for building the driver](https://github.com/metabase/metabase/wiki/Writing-a-Driver:-Packaging-a-Driver-&-Metabase-Plugin-Basics). Alternatively, download a pre-release jar.
3. Copy `clickhouse.metabase-driver.jar` into your Metabase `plugins` directory
5. Start Metabase, e.g. by invoking `lein run` in your Metabase directory.

If you want to develop simply create a symbolic link from the Metabase `modules/drivers` directory to the root of the driver directory.

## Do the Docker Dance

### Mount plugins directory

This is the recommended way, according to the [fine manual](https://www.metabase.com/docs/latest/operations-guide/running-metabase-on-docker.html#adding-external-dependencies-or-plugins):

```
  docker run -d -p 3000:3000 \
  --mount type=bind,source=/path/to/plugins,destination=/plugins \
  --name metabase metabase/metabase
```

### Roll your own

In an empty directory, create your Dockerfile, e.g. `Dockerfile-clickhouse`

```
FROM metabase/metabase:latest
ADD https://github.com/enqueue/metabase-clickhouse-driver/releases/download/0.6/clickhouse.metabase-driver.jar /plugins/
RUN chmod 744 /plugins/clickhouse.metabase-driver.jar
```

Assemble

```
docker build -f Dockerfile-clickhouse -t foo/metabase-with-clickhouse .
```

Run

```
docker run --rm -d=false -p 3000:3000 --name metabase foo/metabase-with-clickhouse
```

Please refer to [the fine Metabase operations manual](https://www.metabase.com/docs/latest/operations-guide/running-metabase-on-docker.html) to find out how to operate a dockerized Metabase with a regular database.


# Contributing
* Report any issues you encounter during operations
* Create a pull request, preferably with a test or five
* See the very useful documentation by the Metabase team: [Writing A Driver](https://github.com/metabase/metabase/wiki/Writing-A-Driver)

# License
The contents of this repository are made available under the GNU Affero General Public License v3.0 (AGPL), see [LICENSE](https://github.com/enqueueu/metabase-clickhouse-driver/blob/master/LICENSE). Unless explicitly stated differently in the respective file, all files are `Copyright 2018-2020 the metabase-clickhouse-driver contributors`.
