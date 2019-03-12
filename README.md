# metabase-clickhouse-driver
MetaBase driver for the ClickHouse database

# Testing

Please refer to the extensive documentation available in the MetaBase Wiki: [Writing A Driver](https://github.com/metabase/metabase/wiki/Writing-A-Driver)

1. Clone [metabase repository](https://github.com/metabase/metabase)
2. Clone this repository, and follow [the instructions for building the driver](https://github.com/metabase/metabase/wiki/Writing-a-Driver:-Packaging-a-Driver-&-Metabase-Plugin-Basics). Alternatively, download a pre-release jar.
3. Copy `clickhouse.metabase-driver.jar` into your MetaBase `plugins` directory
4. Start MetaBase, e.g. by invoking `lein run` in your MetaBase directory.

# History
The request for a ClickHouse MetaBase driver is formulated in [MetaBase issue #3332](https://github.com/metabase/metabase/issues/3332). Some impatient ClickHouse users started development. The MetaBase team is asking driver developers to publish plug-ins and collect some experiences before considering a PR, so here we are.

This driver is based on the following PRs:
* [metabase#8491](https://github.com/metabase/metabase/pull/8491)
* [metabase#8722](https://github.com/metabase/metabase/pull/8722)
* [metabase#9469](https://github.com/metabase/metabase/pull/9469)

The initial source base comprises major contributions from these authors (_the git log has suffered from frequent brutal rebases, please add yourself here, if I missed you!_):

* Bogdan Mukvich (@Badya)
* @tsl-karlp
* Andrew Grigorev (@ei-grad)
