(ns metabase.driver.clickhouse-test-utils
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.test :refer :all]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.models [database :refer [Database]]]
   [metabase.query-processor-test :as qp.test]
   [metabase.sync.sync-metadata :as sync-metadata]
   [metabase.test.data [interface :as tx]]
   [toucan.util.test :as tt]))

(defn rows-without-index
  "Remove the Metabase index which is the first column in the result set"
  [query-result]
  (map #(drop 1 %) (qp.test/rows query-result)))

(defn drop-if-exists-and-create-db!
  "Drop a ClickHouse database named `db-name` if it already exists;
   then create a new empty one with that name."
  [db-name]
  (let [spec (sql-jdbc.conn/connection-details->spec
              :clickhouse
              (tx/dbdef->connection-details :clickhouse :server nil))]
    (jdbc/execute! spec [(format "DROP DATABASE IF EXISTS \"%s\";" db-name)])
    (jdbc/execute! spec [(format "CREATE DATABASE \"%s\";" db-name)])))

(defn- metabase-test-db-details
  []
  (tx/dbdef->connection-details
   :clickhouse :db {:database-name "metabase_test"}))

(defn- create-metabase-test-db!
  "Create a ClickHouse database called `metabase_test` and initialize some test data"
  []
  (drop-if-exists-and-create-db! "metabase_test")
  (jdbc/with-db-connection
    [conn (sql-jdbc.conn/connection-details->spec :clickhouse (metabase-test-db-details))]
    (doseq [sql [(str "CREATE TABLE `metabase_test`.`enums_test` ("
                      " enum1 Enum8('foo' = 0, 'bar' = 1, 'foo bar' = 2),"
                      " enum2 Enum16('click' = 0, 'house' = 1),"
                      " enum3 Enum8('qaz' = 42, 'qux' = 23)"
                      ") ENGINE = Memory")
                 (str "INSERT INTO `metabase_test`.`enums_test` (\"enum1\", \"enum2\", \"enum3\") VALUES"
                      "  ('foo', 'house', 'qaz'),"
                      "  ('foo bar', 'click', 'qux'),"
                      "  ('bar', 'house', 'qaz');")
                 (str "CREATE TABLE `metabase_test`.`ipaddress_test` ("
                      " ipvfour Nullable(IPv4), ipvsix Nullable(IPv6)) Engine = Memory")
                 (str "INSERT INTO `metabase_test`.`ipaddress_test` (ipvfour, ipvsix) VALUES"
                      " (toIPv4('127.0.0.1'), toIPv6('127.0.0.1')),"
                      " (toIPv4('0.0.0.0'), toIPv6('0.0.0.0')),"
                      " (null, null);")
                 (str "CREATE TABLE `metabase_test`.`boolean_test` ("
                      " ID Int32, b1 Bool, b2 Nullable(Bool)) ENGINE = Memory")
                 (str "INSERT INTO `metabase_test`.`boolean_test` (ID, b1, b2) VALUES"
                      " (1, true, true),"
                      " (2, false, true),"
                      " (3, true, false);")
                 (str "CREATE TABLE `metabase_test`.`maps_test`"
                      " (m Map(String, UInt64)) ENGINE = Memory;")
                 (str "INSERT INTO `metabase_test`.`maps_test` VALUES"
                      " ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});")
                 ;; Used for testing that AggregateFunction columns are excluded,
                 ;; while SimpleAggregateFunction columns are preserved
                 (str "CREATE TABLE `metabase_test`.`aggregate_functions_filter_test` ("
                      " idx UInt8, a AggregateFunction(uniq, String), lowest_value SimpleAggregateFunction(min, UInt8),"
                      " count SimpleAggregateFunction(sum, Int64)"
                      ") ENGINE Memory;")
                 (str "INSERT INTO `metabase_test`.`aggregate_functions_filter_test`"
                      " (idx, lowest_value, count) VALUES (42, 144, 255255);")
                 ;; Materialized views (testing .inner tables exclusion)
                 (str "CREATE TABLE `metabase_test`.`wikistat` ("
                      " `date` Date,"
                      " `project` LowCardinality(String),"
                      " `hits` UInt32"
                      ") ENGINE = Memory;")
                 (str "CREATE MATERIALIZED VIEW `metabase_test`.`wikistat_mv` ENGINE=Memory AS"
                      " SELECT date, project, sum(hits) AS hits FROM `metabase_test`.`wikistat`"
                      " GROUP BY date, project;")
                 (str "INSERT INTO `metabase_test`.`wikistat` VALUES"
                      " (now(), 'foo', 10),"
                      " (now(), 'bar', 10),"
                      " (now(), 'bar', 20);")
                 ;; Used in sum-where tests
                 (str "CREATE TABLE `metabase_test`.`sum_if_test_int`"
                      "(id Int64, int_value Int64, discriminator String) ENGINE = Memory;")
                 (str "INSERT INTO `metabase_test`.`sum_if_test_int` VALUES"
                      "(1, 1, 'foo'), (2, 1, 'foo'), (3, 3, 'bar'), (4, 5, 'bar');")
                 (str "CREATE TABLE `metabase_test`.`sum_if_test_float`"
                      "(id Int64, float_value Float64, discriminator String) ENGINE = Memory;")
                 (str "INSERT INTO `metabase_test`.`sum_if_test_float` VALUES"
                      "(1, 1.1, 'foo'), (2, 1.44, 'foo'), (3, 3.5, 'bar'), (4, 5.77, 'bar');")
                 ;; Temporal bucketing tests
                 (str "CREATE TABLE `metabase_test`.`temporal_bucketing`"
                      "(start_of_year DateTime, mid_of_year DateTime, end_of_year DateTime) ENGINE = Memory;")
                 (str "INSERT INTO `metabase_test`.`temporal_bucketing` VALUES"
                      "('2022-01-01 00:00:00', '2022-06-20 06:32:54', '2022-12-31 23:59:59');")]]
      (jdbc/execute! conn [sql]))))

(defn do-with-metabase-test-db
  "Execute a test function using the test dataset from Metabase itself"
  {:style/indent 0}
  [f]
  (create-metabase-test-db!)
  (tt/with-temp Database
    [database
     {:engine :clickhouse :details (metabase-test-db-details)}]
    (sync-metadata/sync-db-metadata! database)
    (f database)))
