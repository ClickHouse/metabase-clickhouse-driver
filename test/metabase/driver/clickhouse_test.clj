(ns metabase.driver.clickhouse-test
  "Tests for specific behavior of the ClickHouse driver."
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [cljc.java-time.format.date-time-formatter :as date-time-formatter]
            [cljc.java-time.offset-date-time :as offset-date-time]
            [cljc.java-time.temporal.chrono-unit :as chrono-unit]
            [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.clickhouse-data-types-test]
            [metabase.driver.clickhouse-introspection-test]
            [metabase.driver.clickhouse-set-role-test]
            [metabase.driver.clickhouse-substitution-test]
            [metabase.driver.clickhouse-temporal-bucketing-test]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.query-processor :as qp]
            [metabase.query-processor.test-util :as qp.test]
            [metabase.test :as mt]
            [metabase.test.data :as data]
            [metabase.test.data [interface :as tx]]
            [metabase.test.data.clickhouse :as ctd]
            [taoensso.nippy :as nippy]))

(set! *warn-on-reflection* true)

(use-fixtures :once ctd/create-test-db!)

(deftest ^:parallel clickhouse-version
  (mt/test-driver
   :clickhouse
   (let [version (driver/dbms-version :clickhouse (mt/db))]
     (is (number? (get-in version [:semantic-version :major])))
     (is (number? (get-in version [:semantic-version :minor])))
     (is (string? (get    version :version))))))

(deftest ^:parallel clickhouse-server-timezone
  (mt/test-driver
   :clickhouse
   (is (= "UTC"
          (let [details (tx/dbdef->connection-details :clickhouse :db {:database-name "default"})
                spec    (sql-jdbc.conn/connection-details->spec :clickhouse details)]
            (driver/db-default-timezone :clickhouse spec))))))

(deftest ^:parallel clickhouse-now-converted-to-timezone
  (mt/test-driver
   :clickhouse
   (let [[[utc-now shanghai-now]]
         (qp.test/rows
          (qp/process-query
           (mt/native-query
            {:query "SELECT now(), now('Asia/Shanghai')"})))]
     (testing "there is always eight hour difference in time between UTC and Asia/Beijing"
       (is (= 8
              (chrono-unit/between
               chrono-unit/hours
               (offset-date-time/parse utc-now date-time-formatter/iso-offset-date-time)
               (offset-date-time/parse shanghai-now date-time-formatter/iso-offset-date-time))))))))

(deftest ^:parallel clickhouse-connection-string
  (testing "connection with no additional options"
    (is (= ctd/default-connection-params
           (sql-jdbc.conn/connection-details->spec
            :clickhouse
            {}))))
  (testing "custom connection with additional options"
    (is (= (merge
            ctd/default-connection-params
            {:subname "//myclickhouse:9999/foo?sessionTimeout=42"
             :user "bob"
             :password "qaz"
             :use_no_proxy true
             :ssl true})
           (sql-jdbc.conn/connection-details->spec
            :clickhouse
            {:host "myclickhouse"
             :port 9999
             :user "bob"
             :password "qaz"
             :dbname "foo"
             :use-no-proxy true
             :additional-options "sessionTimeout=42"
             :ssl true}))))
  (testing "nil dbname handling"
    (is (= ctd/default-connection-params
           (sql-jdbc.conn/connection-details->spec
            :clickhouse
            {:dbname nil})))))

(deftest ^:parallel clickhouse-tls
  (mt/test-driver
   :clickhouse
   (let [working-dir (System/getProperty "user.dir")
         cert-path (str working-dir "/modules/drivers/clickhouse/.docker/clickhouse/single_node_tls/certificates/ca.crt")
         additional-options (str "sslrootcert=" cert-path)]
     (testing "simple connection with a single database"
       (is (= "UTC"
              (driver/db-default-timezone
               :clickhouse
               (sql-jdbc.conn/connection-details->spec
                :clickhouse
                {:ssl true
                 :host "server.clickhouseconnect.test"
                 :port 8443
                 :additional-options additional-options})))))
     (testing "connection with multiple databases"
       (is (= "UTC"
              (driver/db-default-timezone
               :clickhouse
               (sql-jdbc.conn/connection-details->spec
                :clickhouse
                {:ssl true
                 :host "server.clickhouseconnect.test"
                 :port 8443
                 :dbname "default system"
                 :additional-options additional-options}))))))))

(deftest ^:parallel clickhouse-nippy
  (mt/test-driver
   :clickhouse
   (testing "UnsignedByte"
     (let [value (com.clickhouse.data.value.UnsignedByte/valueOf "214")]
       (is (= value (nippy/thaw (nippy/freeze value))))))
   (testing "UnsignedShort"
     (let [value (com.clickhouse.data.value.UnsignedShort/valueOf "62055")]
       (is (= value (nippy/thaw (nippy/freeze value))))))
   (testing "UnsignedInteger"
     (let [value (com.clickhouse.data.value.UnsignedInteger/valueOf "4748364")]
       (is (= value (nippy/thaw (nippy/freeze value))))))
   (testing "UnsignedLong"
     (let [value (com.clickhouse.data.value.UnsignedLong/valueOf "84467440737095")]
       (is (= value (nippy/thaw (nippy/freeze value))))))))

(deftest ^:parallel clickhouse-query-formatting
  (mt/test-driver
   :clickhouse
   (let [query             (data/mbql-query venues {:fields [$id] :order-by [[:asc $id]] :limit 5})
         {compiled :query} (qp/compile-and-splice-parameters query)
         pretty            (driver/prettify-native-form :clickhouse compiled)]
     (testing "compiled"
       (is (= "SELECT `test_data`.`venues`.`id` AS `id` FROM `test_data`.`venues` ORDER BY `test_data`.`venues`.`id` ASC LIMIT 5" compiled)))
     (testing "pretty"
       (is (= "SELECT\n  `test_data`.`venues`.`id` AS `id`\nFROM\n  `test_data`.`venues`\nORDER BY\n  `test_data`.`venues`.`id` ASC\nLIMIT\n  5" pretty))))))

(deftest ^:parallel clickhouse-can-connect
  (mt/test-driver
   :clickhouse
   (doall
    (for [[username password] [["default" ""] ["user_with_password" "foo@bar!"]]
          database            ["default" "Special@Characters~" "'Escaping'"]]
      (testing (format "User `%s` can connect to `%s`" username database)
        (let [details (merge {:user username :password password}
                             (tx/dbdef->connection-details :clickhouse :db {:database-name database}))]
          (is (true? (driver/can-connect? :clickhouse details)))))))))
