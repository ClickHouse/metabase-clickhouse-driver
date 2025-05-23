# 1.53.4

### Improvements

* The JDBC driver was updated to [0.8.4](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.8.4). This fixes [#309](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/309), [#300](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/300), [#297](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/297).

# 1.53.3

### Improvements

* If ClickHouse instance hostname was specified including `http://` or `https://` schema (e.g. `https://sub.example.com`), it will be automatically handled and removed by the driver, instead of failing with a connection error.
* The JDBC driver was updated to [0.8.2](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.8.2)

# 1.53.2

### Bug fixes

* The JDBC driver was updated to [0.8.1](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.8.1) to fix errors in queries with CTEs ([#297](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/297), [#288](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/288), [tadeboro](https://github.com/tadeboro)).

# 1.53.1

### Bug fixes

* Fix unsigned integers overflow ([#293](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/293))

# 1.53.0

### Improvements

* Adds Metabase 0.53.x support ([#287](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/287), [dpsutton](https://github.com/dpsutton)).

### Bug fixes

* Fixed OOB exception on CSV insert caused by an incompatibility with JDBC v2 ([#286](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/286), [wotbrew](https://github.com/wotbrew)).

# 1.52.0

- Formal Metabase 0.52.x+ support
- The driver now uses JDBC v2 ([0.8.0](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.8.0))
- Various improvements to handling of datetimes with timezones
- `:convert-timezone` feature is disabled for now.
- Added `max-open-connections` setting under "advanced options"; default is 100.

# 1.51.0

Adds Metabase 0.51.x support.

# 1.50.7

### Improvements

* Added a configuration field (under the "advanced options", hidden by default) to override certain ClickHouse settings ([#272](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/272)).

# 1.50.6

### Bug fixes

* Fixed null pointer exception on CSV insert ([#268](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/268), [crisptrutski](https://github.com/crisptrutski)).

# 1.50.5

### Bug fixes

* Fixed an error that could occur while setting roles containing hyphens for connection impersonation ([#266](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/266), [sharankow](https://github.com/sharankow)).

# 1.50.4

### Bug fixes

* Fixed an error while uploading a CSV with an offset datetime column ([#263](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/263), [crisptrutski](https://github.com/crisptrutski)).

# 1.50.3

### Improvements

* The driver no longer explicitly sets `allow_experimental_analyzer=0` settings on the connection level; the [new ClickHouse analyzer](https://clickhouse.com/docs/en/operations/analyzer) is now enabled by default.

# 1.50.2

### Bug fixes

* Fixed Array inner type introspection, which could cause reduced performance when querying tables containing arrays. ([#257](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/257))

# 1.50.1

### New features

* Enabled `:set-timezone` ([docs](https://www.metabase.com/docs/latest/configuring-metabase/localization#report-timezone)) Metabase feature in the driver. ([#200](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/200))
* Enabled `:convert-timezone` ([docs](https://www.metabase.com/docs/latest/questions/query-builder/expressions/converttimezone)) Metabase feature in the driver. ([#254](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/254))

### Other

* The driver now uses [`session_timezone` ClickHouse setting](https://clickhouse.com/docs/en/operations/settings/settings#session_timezone). This is necessary to support the `:set-timezone` and `:convert-timezone` features; however, this setting [was introduced in 23.6](https://clickhouse.com/docs/en/whats-new/changelog/2023#236), which makes it the minimal required ClickHouse version to work with the driver.

# 1.50.0

After Metabase 0.50.0, a new naming convention exists for the driver's releases. The new one is intended to reflect the Metabase version the driver is supposed to run on. For example, the driver version 1.50.0 means that it should be used with Metabase v0.50.x or Metabase EE 1.50.x _only_, and it is _not guaranteed_ that this particular version of the driver can work with the previous or the following versions of Metabase.

### New features

* Added Metabase 0.50.x support.

### Improvements

* Bumped the JDBC driver to [0.6.1](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.6.1).

### Bug fixes

* Fixed the issue where the connection impersonation feature support could be incorrectly reported as disabled.

### Other

* The new ClickHouse analyzer, [which is enabled by default in 24.3+](https://clickhouse.com/blog/clickhouse-release-24-03#analyzer-enabled-by-default), is disabled for the queries executed by the driver, as it shows some compatibilities with the queries generated by Metabase (see [this issue](https://github.com/ClickHouse/ClickHouse/issues/64487) for more details).
* The `:window-functions/offset` Metabase feature is currently disabled, as the default implementation generates queries incompatible with ClickHouse. See [this issue](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/245) for tracking.

# 1.5.1

Metabase 0.49.14+ only.

### Bug fixes

* Fixed the issue where the Metabase instance could end up broken if the ClickHouse instance was _stopped_ during the upgrade to 1.5.0. ([#242](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/242))
* Fixed variables substitution with Nullable Date, Date32, DateTime, and DateTime64 columns, where the generated query could fail with NULL values in the database due to an incorrect cast call. ([#243](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/243))

# 1.5.0

Metabase 0.49.14+ only.

### New features

* Added [Metabase CSV Uploads feature](https://www.metabase.com/docs/latest/databases/uploads) support, which is currently enabled with ClickHouse Cloud only. On-premise deployments support will be added in the next release. ([calherries](https://github.com/calherries), [#236](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/236), [#238](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/238))
* Added [Metabase connection impersonation feature](https://www.metabase.com/learn/permissions/impersonation) support. This feature will be enabled by the driver only if ClickHouse version 24.4+ is detected. ([#219](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/219))

### Improvements

* Proper role setting support on cluster deployments (related issue: [#192](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/192)).
* Bump the JDBC driver to [0.6.0-patch5](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.6.0-patch5).

### Bug fixes

* Fixed missing data for the last day when using filters with DateTime columns. ([#202](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/202), [#229](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/229))

# 1.4.0

### New features
* Metabase 0.49.x support.

### Bug fixes
* Fixed an incorrect substitution for the current day filter with DateTime columns. ([#216](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/216))

# 1.3.4

### New features

* If introspected ClickHouse version is lower than 23.8, the driver will not use [startsWithUTF8](https://clickhouse.com/docs/en/sql-reference/functions/string-functions#startswithutf8) and fall back to its [non-UTF8 counterpart](https://clickhouse.com/docs/en/sql-reference/functions/string-functions#startswith) instead. There is a drawback in this compatibility mode: potentially incorrect filtering results when working with non-latin strings. If your use case includes filtering by columns with such strings and you experience these issues, consider upgrading your ClickHouse server to 23.8+. ([#224](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/224))

# 1.3.3

### Bug fixes
* Fixed an issue where it was not possible to create a connection with multiple databases using TLS. ([#215](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/215))

# 1.3.2

### Bug fixes
* Remove `can-connect?` method override which could cause issues with editing or creating new connections. ([#212](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/212))


# 1.3.1

### Bug fixes
* Fixed incorrect serialization of `Array(UInt8)` columns ([#209](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/209))

# 1.3.0

### New features
* Metabase 0.48.x support

### Bug fixes
* Fixed last/next minutes/hours filters with variables creating incorrect queries due to unnecessary `CAST col AS date` call.

# 1.2.5

Metabase 0.47.7+ only.

### New features
* Added [datetimeDiff](https://www.metabase.com/docs/latest/questions/query-builder/expressions/datetimediff) function support ([#117](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/117))

# 1.2.4

Metabase 0.47.7+ only.

### Bug fixes
* Fixed UI question -> SQL conversion creating incorrect queries due to superfluous spaces in columns/tables/database names.

# 1.2.3

### Bug fixes

* Fixed `LowCardinality(Nullable)` types introspection, where it was incorrectly reported as `type/*` ([#203](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/203))

# 1.2.2

### Bug fixes
* Removed forward slash from serialized IPv4/IPv6 columns. NB: IPv4/IPv6 columns are temporarily resolved as `type/TextLike` instead of `type/IPAddress` base type due to an unexpected result in Metabase 0.47 type check.
* Removed superfluous CAST calls from generated queries that use Date* columns and/or intervals

# 1.2.1
### New features
* Use HoneySQL2 in the driver

# 1.2.0

### New features
* Metabase 0.47 support
* Connection impersonation support (0.47 feature)

### Bug fixes
* More correct general database type -> base type mapping
* `DateTime64` is now correctly mapped to `:type/DateTime`
* `database-required` field property is now correctly set to `true` if a field is not `Nullable`

# 1.1.7

### New features

* JDBC driver upgrade (v0.4.1 -> [v0.4.6](https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.4.6))
* Support DateTime64 by [@lucas-tubi](https://github.com/lucas-tubi) ([#165](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/165))
* Use native `startsWith`/`endsWith` instead of `LIKE str%`/`LIKE %str`

# 1.1.6

### Bug fixes

* Fixed temporal bucketing issues (see [#155](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/155))

# 1.1.5

### Bug fixes

* Fixed Nippy error on cached questions (see [#147](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/147))

# 1.1.4

### Bug fixes

* Fixed `sum-where` behavior where previously it could not be applied to Int columns (see [#156](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/156))

# 1.1.3

### New features

* Hide `.inner` tables of Materialized Views.
* Resolve `Map` base type to `type/Dictionary`.
* Database name can now contain multiple schemas in the UI field (space-separated by default), which tells the driver to scan selected databases. Separator can be set in `metabase.driver.clickhouse/SEPARATOR`. (@veschin)

# 1.1.2

### Bug fixes

* Now the driver is able to scan and work with `SimpleAggregateFunction` columns: those were excluded by mistake in 1.0.2.


# 1.1.1

### New features

* Metabase 0.46.x compatibility.
* Added [cljc.java-time](https://clojars.org/com.widdindustries/cljc.java-time) to dependencies, as it is no longer loaded by Metabase.

# 1.1.0

### New features

* Update JDBC driver to v0.4.1.
* Use new `product_name` additional option instead of `client_name`
* Replace `sql-jdbc.execute/read-column [:clickhouse Types/ARRAY]` with `sql-jdbc.execute/read-column-thunk [:clickhouse Types/ARRAY]` to be compatible with Metabase 0.46 breaking changes once it is released.

### Bug fixes

* Fix `sql-jdbc.execute/read-column-thunk [:clickhouse Types/TIME]` return type.

# 1.0.4

### New features

* Adds a new "Scan all databases" UI toggle (disabled by default), which tells the driver to scan all available databases (excluding `system` and `information_schema`) instead of only the database it is connected to.
* Database input moved below host/port/username/password in the UI.

# 1.0.3

### Bug fixes

* Fixed NPE that could be thrown by the driver in case of empty database name input.

# 1.0.2

### Bug fixes

* As the underlying JDBC driver version does not support columns with `(Simple)AggregationFunction` type, these columns are now excluded from the table metadata and data browser result sets to prevent sync or data browsing errors.

# 1.0.1

### Bug fixes

* Boolean base type inference fix by [@s-huk](https://github.com/s-huk) (see [#134](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/134))

# 1.0.0

Formal stable release milestone.

### New features

* Added HTTP User-Agent (via clickhouse-jdbc `client_name` setting) with the plugin info according to the [language client spec](https://docs.google.com/document/d/1924Dvy79KXIhfqKpi1EBVY3133pIdoMwgCQtZ-uhEKs/edit#heading=h.ah33hoz5xei2)

# 0.9.2

### New features

* Allow to bypass system-wide proxy settings [#120](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/120)

It's the first plugin release from the ClickHouse organization.

From now on, the plugin is distributed under the Apache 2.0 License.

# 0.9.1

### New features

* Metabase 0.45.x compatibility [#107](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/107)
* Added SSH tunnel option [#116](https://github.com/ClickHouse/metabase-clickhouse-driver/pull/116)

# 0.9.0

### New features

* Using https://github.com/ClickHouse/clickhouse-jdbc `v0.3.2-patch11`

### Bug fixes

* URLs with underscores [#23](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/23)
* `now()` timezones issues [#81](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/81)
* Boolean errors [#88](https://github.com/ClickHouse/metabase-clickhouse-driver/issues/88)

NB: there are messages like this in the Metabase logs

```
2022-12-07 11:20:58,056 WARN internal.ClickHouseConnectionImpl :: [JDBC Compliant Mode] Transaction is not supported. You may change jdbcCompliant to false to throw SQLException instead.
2022-12-07 11:20:58,056 WARN internal.ClickHouseConnectionImpl :: [JDBC Compliant Mode] Transaction [ce0e121a-419a-4414-ac39-30f79eff7afd] (0 queries & 0 savepoints) is committed.
```

Unfortunately, this is the behaviour of the underlying JDBC driver now.

Please consider raising the log level for `com.clickhouse.jdbc.internal.ClickHouseConnectionImpl` to `ERROR`.

# 0.8.3

### New features

* Enable additional options for ClickHouse connection

# 0.8.2

### New features

* Compatibility with Metabase 0.44
