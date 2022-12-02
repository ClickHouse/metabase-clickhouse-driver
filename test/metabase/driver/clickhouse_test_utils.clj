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
  (jdbc/with-db-connection [conn (sql-jdbc.conn/connection-details->spec :clickhouse (metabase-test-db-details))]
    (doseq [sql [(str "CREATE TABLE `metabase_test`.`enums_test` ("
                      " enum1 Enum8('foo' = 0, 'bar' = 1, 'foo bar' = 2),"
                      " enum2 Enum16('click' = 0, 'house' = 1)"
                      ") ENGINE = Memory")
                 (str "INSERT INTO `metabase_test`.`enums_test` (\"enum1\", \"enum2\") VALUES"
                      "  ('foo', 'house'),"
                      "  ('foo bar', 'click'),"
                      "  ('bar', 'house');")
                 (str "CREATE TABLE `metabase_test`.`ipaddress_test` ("
                      " ipvfour Nullable(IPv4), ipvsix Nullable(IPv6)) Engine = Memory")
                 (str "INSERT INTO `metabase_test`.`ipaddress_test` (ipvfour, ipvsix) VALUES"
                      " (toIPv4('127.0.0.1'), toIPv6('127.0.0.1')),"
                      " (toIPv4('0.0.0.0'), toIPv6('0.0.0.0')),"
                      " (null, null);")]]
      (jdbc/execute! conn [sql]))))

(defn do-with-metabase-test-db
  {:style/indent 0}
  [f]
  (create-metabase-test-db!)
  (tt/with-temp Database
    [database
     {:engine :clickhouse :details (metabase-test-db-details)}]
    (sync-metadata/sync-db-metadata! database)
    (f database)))
