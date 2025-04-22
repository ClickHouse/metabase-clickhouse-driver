<p align="center" style="font-size:300%">
<img src="https://www.metabase.com/images/logo.svg" width="200px" align="center">
<img src=".static/clickhouse.svg" width="180px" align="center">
<h1 align="center">ClickHouse driver for Metabase</h1>
</p>
<br/>
<p align="center">
<a href="https://raw.githubusercontent.com/enqueue/metabase-clickhouse-driver/master/LICENSE">
<img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg">
</a>
</p>

## About

The Metabase team promoted the ClickHouse driver to the core level as of Metabase 54 ([release notes](https://github.com/metabase/metabase/releases/tag/v0.54.1), [driver source](https://github.com/metabase/metabase/tree/v0.54.x/modules/drivers/clickhouse)).

For the end user, this means the following:

- Installing the driver manually is unnecessary, as it is now bundled with Metabase.
- The Metabase team will continue maintaining the driver; please report new issues in the [main Metabase repository](https://github.com/metabase/metabase/issues).

The ClickHouse team recommends avoiding older Metabase versions (53.x and earlier) with manual driver installation; instead, please use the updated Metabase distribution with the driver built-in.

## History

The request for a ClickHouse Metabase driver was formulated in 2016 in [Metabase issue #3332](https://github.com/metabase/metabase/issues/3332). Several impatient ClickHouse users started the development in the main Metabase repo. In March 2019, after releasing the plugin SDK, the Metabase team [asked to publish the driver separately](https://github.com/metabase/metabase/pull/8491#issuecomment-471721980) in its own repository, and later that month, with Felix Mueller ([@enqueue](https://github.com/enqueue)) leading the efforts, the [initial version of the driver](https://github.com/ClickHouse/metabase-clickhouse-driver/releases/tag/v0.1) was out.

The original implementation of the driver was based on the following pull requests:

- [metabase#8491](https://github.com/metabase/metabase/pull/8491)
- [metabase#8722](https://github.com/metabase/metabase/pull/8722)
- [metabase#9469](https://github.com/metabase/metabase/pull/9469)

The source base in these PRs comprises major contributions from these authors:

- [@tsl-karlp](https://github.com/tsl-karlp)
- Andrew Grigorev ([@ei-grad](https://github.com/ei-grad))
- Bogdan Mukvich ([@Badya](https://github.com/Badya))
- Felix Mueller ([@enqueue](https://github.com/enqueue))

## Special thanks

Felix Mueller ([@enqueue](https://github.com/enqueue)) was the sole maintainer of the project from 2019 to 2022 before transferring the ownership to ClickHouse.
