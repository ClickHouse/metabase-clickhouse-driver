(ns metabase.driver.clickhouse-test
  "SET ROLE (connection impersonation feature) tests on with single node or on-premise cluster setups."
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.sql :as driver.sql]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.test :as mt]
            [metabase.test.data [interface :as tx]]
            [metabase.test.data.clickhouse :as ctd]))

(defn- set-role-test!
  [details-map]
  (let [default-role (driver.sql/default-database-role :clickhouse nil)
        spec         (sql-jdbc.conn/connection-details->spec :clickhouse details-map)]
    (testing "default role is NONE"
      (is (= default-role "NONE")))
    (testing "does not throw with an existing role"
      (sql-jdbc.execute/do-with-connection-with-options
       :clickhouse spec nil
       (fn [^java.sql.Connection conn]
         (driver/set-role! :clickhouse conn "metabase_test_role")))
      (is true))
    (testing "does not throw with the default role"
      (sql-jdbc.execute/do-with-connection-with-options
       :clickhouse spec nil
       (fn [^java.sql.Connection conn]
         (driver/set-role! :clickhouse conn default-role)))
      (is true))
    (testing "throws when assigning a non-existent role"
      (is (thrown? Exception
                   (sql-jdbc.execute/do-with-connection-with-options
                    :clickhouse spec nil
                    (fn [^java.sql.Connection conn]
                      (driver/set-role! :clickhouse conn "asdf"))))))))

(deftest clickhouse-set-role-single-node
  (mt/test-driver
   :clickhouse
   (let [statements ["DROP DATABASE metabase_test_role_db"
                     "CREATE DATABASE IF NOT EXISTS `metabase_test_role_db`;"
                     "CREATE OR REPLACE TABLE `metabase_test_role_db`.`some_table` (i Int32) ENGINE = MergeTree ORDER BY (i);"
                     "INSERT INTO `metabase_test_role_db`.`some_table` VALUES (42), (144);"
                     "CREATE ROLE IF NOT EXISTS `metabase_test_role`;"
                     "CREATE USER IF NOT EXISTS `metabase_test_user` NOT IDENTIFIED;"
                     "GRANT SELECT ON `metabase_test_role_db`.* TO `metabase_test_role`;"
                     "GRANT `metabase_test_role` TO `metabase_test_user`;"]]
     (ctd/exec-statements statements {})
     (set-role-test! {:user "metabase_test_user"}))))

;;(merge {:user "metabase_test_user"}
                            ;;(tx/dbdef->connection-details :clickhouse :db {:database-name "default"}))
(deftest clickhouse-set-role-on-premise-cluster
  (mt/test-driver
   :clickhouse
   (let [statements ["DROP DATABASE IF EXISTS metabase_test_role_db ON CLUSTER 'test_cluster'"
                     "CREATE DATABASE `metabase_test_role_db` ON CLUSTER 'test_cluster';"
                     "CREATE TABLE `metabase_test_role_db`.`some_table` ON CLUSTER 'test_cluster' (i Int32)
                      ENGINE ReplicatedMergeTree('/clickhouse/{cluster}/tables/{database}/{table}/{shard}', '{replica}')
                      ORDER BY (i);"
                     "INSERT INTO `metabase_test_role_db`.`some_table` VALUES (42), (144);"
                     "CREATE ROLE IF NOT EXISTS `metabase_test_role` ON CLUSTER 'test_cluster';"
                     "CREATE USER IF NOT EXISTS `metabase_test_user` ON CLUSTER 'test_cluster' NOT IDENTIFIED;"
                     "GRANT ON CLUSTER 'test_cluster' SELECT ON `metabase_test_role_db`.* TO `metabase_test_role`;"
                     "GRANT ON CLUSTER 'test_cluster' `metabase_test_role` TO `metabase_test_user`;"]]
     (ctd/exec-statements statements {:port 8127})
     (set-role-test! {:user "metabase_test_user" :port 8127}))))
