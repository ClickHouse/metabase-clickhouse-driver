ARG METABASE_VERSION
FROM metabase/metabase:$METABASE_VERSION
COPY ./clickhouse.metabase-driver.jar /plugins/clickhouse.metabase-driver.jar
RUN chmod 744 /plugins/clickhouse.metabase-driver.jar
