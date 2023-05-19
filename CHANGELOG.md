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
