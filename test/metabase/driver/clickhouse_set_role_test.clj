(ns metabase.driver.clickhouse-test
  "SET ROLE (connection impersonation feature) tests on with single node or on-premise cluster setups."
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.sql :as driver.sql]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.query-processor.store :as qp.store]
            [metabase.test :as mt]
            [metabase.test.data.clickhouse :as ctd]))

(set! *warn-on-reflection* true)

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
         (driver/set-role! :clickhouse conn default-role)
         (fn [^java.sql.Connection conn]
           (driver/set-role! :clickhouse conn default-role)
           (with-open [stmt (.prepareStatement conn "SELECT * FROM `metabase_test_role_db`.`some_table` ORDER BY i ASC;")
                       rset (.executeQuery stmt)]
             (is (.next rset) true)
             (is (.getInt rset 1) 42)
             (is (.next rset) true)
             (is (.getInt rset 1) 144)
             (is (.next rset) false)))))
      (is true))))

(defn- set-role-throws-test!
  [details-map]
  (testing "throws when assigning a non-existent role"
    (is (thrown? Exception
                 (sql-jdbc.execute/do-with-connection-with-options
                  :clickhouse (sql-jdbc.conn/connection-details->spec :clickhouse details-map) nil
                  (fn [^java.sql.Connection conn]
                    (driver/set-role! :clickhouse conn "asdf")))))))

(defn- do-with-new-metadata-provider
  [thunk]
  (binding [qp.store/*TESTS-ONLY-allow-replacing-metadata-provider* true]
    (qp.store/with-metadata-provider (mt/id)
      (thunk))))

(deftest clickhouse-set-role
  (mt/test-driver
   :clickhouse
   (let [user-details               {:user "metabase_test_user"}
         ;; See docker-compose.yml for the port mappings
         ;; 24.4+
         port-details               {:port 8123}
         ;; 23.3
         port-details-older         {:port 8124}
         ;; 24.4+
         cluster-port-details       {:port 8127}
         ;; 23.3
         cluster-port-details-older {:port 8130}]

     (testing "single node"
       (let [statements ["CREATE DATABASE IF NOT EXISTS `metabase_test_role_db`;"
                         "CREATE OR REPLACE TABLE `metabase_test_role_db`.`some_table` (i Int32) ENGINE = MergeTree ORDER BY (i);"
                         "INSERT INTO `metabase_test_role_db`.`some_table` VALUES (42), (144);"
                         "CREATE ROLE IF NOT EXISTS `metabase_test_role`;"
                         "CREATE USER IF NOT EXISTS `metabase_test_user` NOT IDENTIFIED;"
                         "GRANT SELECT ON `metabase_test_role_db`.* TO `metabase_test_role`;"
                         "GRANT `metabase_test_role` TO `metabase_test_user`;"]]
         (testing "current ClickHouse version"
           (do-with-new-metadata-provider
            (fn []
              (ctd/exec-statements statements port-details)
              (set-role-test!        (merge user-details port-details))
              (set-role-throws-test! (merge user-details port-details)))))
         (testing "older ClickHouse version"
           (do-with-new-metadata-provider
            (fn []
              (ctd/exec-statements statements port-details-older)
              (set-role-test! (merge user-details port-details-older)))))))
     (testing "on-premise cluster"
       (let [statements ["CREATE DATABASE IF NOT EXISTS `metabase_test_role_db` ON CLUSTER 'test_cluster';"
                         "CREATE OR REPLACE TABLE `metabase_test_role_db`.`some_table` ON CLUSTER 'test_cluster' (i Int32)
                                       ENGINE ReplicatedMergeTree('/clickhouse/{cluster}/tables/{database}/{table}/{shard}', '{replica}')
                                       ORDER BY (i);"
                         "INSERT INTO `metabase_test_role_db`.`some_table` VALUES (42), (144);"
                         "CREATE ROLE IF NOT EXISTS `metabase_test_role` ON CLUSTER 'test_cluster';"
                         "CREATE USER IF NOT EXISTS `metabase_test_user` ON CLUSTER 'test_cluster' NOT IDENTIFIED;"
                         "GRANT ON CLUSTER 'test_cluster' SELECT ON `metabase_test_role_db`.* TO `metabase_test_role`;"
                         "GRANT ON CLUSTER 'test_cluster' `metabase_test_role` TO `metabase_test_user`;"]]
         (testing "current ClickHouse version"
           (do-with-new-metadata-provider
            (fn []
              (ctd/exec-statements statements cluster-port-details)
              (set-role-test!        (merge user-details cluster-port-details))
              (set-role-throws-test! (merge user-details cluster-port-details)))))
         (testing "older ClickHouse version"
           (do-with-new-metadata-provider
            (fn []
              (ctd/exec-statements statements cluster-port-details-older)
              (set-role-test! (merge user-details cluster-port-details-older))))))))))
