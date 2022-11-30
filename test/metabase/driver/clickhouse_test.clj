(ns metabase.driver.clickhouse-test
  "Tests for specific behavior of the ClickHouse driver."
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.models [database :refer [Database]]]
            [metabase.query-processor-test :as qp.test]
            [metabase.sync.sync-metadata :as sync-metadata]
            [metabase.test :as mt]
            [metabase.test.data :as data]
            [metabase.test.data [interface :as tx]]
            [metabase.test.util :as tu]
            [toucan.util.test :as tt]))

(deftest clickhouse-timezone-is-utc
  (mt/test-driver :clickhouse (is (= "UTC" (tu/db-timezone-id)))))

(deftest clickhouse-decimal-division-simple
  (mt/test-driver
   :clickhouse
   (is
    (= 21.0
       (-> (data/dataset
            (tx/dataset-definition "metabase_tests_decimal"
                                   ["test-data-decimal"
                                    [{:field-name "my_money"
                                      :base-type {:native "Decimal(12,4)"}}]
                                    [[1.0] [23.1337] [42.0] [42.0]]])
            (data/run-mbql-query test-data-decimal
                                 {:expressions {:divided [:/ $my_money 2]}
                                  :filter [:> [:expression :divided] 1.0]
                                  :breakout [[:expression :divided]]
                                  :order-by [[:desc [:expression :divided]]]
                                  :limit 1}))
           qp.test/first-row
           last
           float)))))

(deftest clickhouse-decimal-even-more-fun
  (mt/test-driver
   :clickhouse
   (is
    (= 1.8155331831916208
       (-> (data/dataset
            (tx/dataset-definition "metabase_tests_decimal"
                                   ["test-data-decimal"
                                    [{:field-name "my_money"
                                      :base-type {:native "Decimal(12,4)"}}]
                                    [[1.0] [23.1337] [42.0] [42.0]]])
            (data/run-mbql-query test-data-decimal
                                 {:expressions {:divided [:/ 42 $my_money]}
                                  :filter [:= $id 2]
                                  :limit 1}))
           qp.test/first-row
           last
           double)))))

(deftest clickhouse-large-decimal64
  (mt/test-driver
   :clickhouse
   (let [[d1 d2] ["12345123.123456789", "78.245"]
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_decimal"
                                              ["test-data-large-decimal64"
                                               [{:field-name "my_money"
                                                 :base-type {:native "Decimal(18,10)"}}]
                                               [[d1] [d2]]])
                       (data/run-mbql-query test-data-large-decimal64 {}))
         rows (qp.test/rows query-result)
         result (map last rows)]
     (is (= [(bigdec d1) (bigdec d2)] result)))))

(deftest clickhouse-array-string
  (mt/test-driver
   :clickhouse
   (is
    (= "['foo','bar']"
       (-> (data/dataset
            (tx/dataset-definition "metabase_tests_array_string"
                                   ["test-data-array-string"
                                    [{:field-name "my_array"
                                      :base-type {:native "Array(String)"}}]
                                    [[(into-array (list "foo" "bar"))]]])
            (data/run-mbql-query test-data-array-string {:limit 1}))
           qp.test/first-row
           last)))))

(deftest clickhouse-array-uint64
  (mt/test-driver
   :clickhouse
   (is
    (= "[23,42]"
       (-> (data/dataset
            (tx/dataset-definition "metabase_tests_array_uint"
                                   ["test-data-array-uint64"
                                    [{:field-name "my_array"
                                      :base-type {:native "Array(UInt64)"}}]
                                    [[(into-array (list 23 42))]]])
            (data/run-mbql-query test-data-array-uint64 {:limit 1}))
           qp.test/first-row
           last)))))

(deftest clickhouse-nullable-strings
  (mt/test-driver
   :clickhouse
   (is (= 2
          (-> (data/dataset (tx/dataset-definition
                             "metabase_tests_nullable_strings"
                             ["test-data-nullable-strings"
                              [{:field-name "mystring" :base-type :type/Text}]
                              [["foo"] ["bar"] ["   "] [""] [nil]]])
                            (data/run-mbql-query test-data-nullable-strings
                                                 {:filter [:is-null $mystring]
                                                  :aggregation [:count]}))
              qp.test/first-row
              last)))))

(deftest clickhouse-nullable-strings-filter-not-null
  (mt/test-driver
   :clickhouse
   (is (= 3
          (-> (data/dataset (tx/dataset-definition
                             "metabase_tests_nullable_strings"
                             ["test-data-nullable-strings"
                              [{:field-name "mystring" :base-type :type/Text}]
                              [["foo"] ["bar"] ["   "] [""] [nil]]])
                            (data/run-mbql-query test-data-nullable-strings
                                                 {:filter [:not-null $mystring]
                                                  :aggregation [:count]}))
              qp.test/first-row
              last)))))

(deftest clickhouse-nullable-strings-filter-value
  (mt/test-driver
   :clickhouse
   (is (= 1
          (-> (data/dataset (tx/dataset-definition
                             "metabase_tests_nullable_strings"
                             ["test-data-nullable-strings"
                              [{:field-name "mystring" :base-type :type/Text}]
                              [["foo"] ["bar"] ["   "] [""] [nil]]])
                            (data/run-mbql-query test-data-nullable-strings
                                                 {:filter [:= $mystring "foo"]
                                                  :aggregation [:count]}))
              qp.test/first-row
              last)))))

(deftest clickhouse-tolowercase-nonlatin-filter-general
  (mt/test-driver
   :clickhouse
   (is (= [[1 "Я_1"] [3 "Я_2"] [4 "Я"]]
          (qp.test/formatted-rows
           [int str]
           :format-nil-values
           (data/dataset
            (tx/dataset-definition
             "metabase_test_lowercases"
             ["test-data-lowercase"
              [{:field-name "mystring" :base-type :type/Text}]
              [["Я_1"] ["R"] ["Я_2"] ["Я"] ["я"] [nil]]])
            (data/run-mbql-query test-data-lowercase
                                 {:filter [:contains $mystring "Я"]})))))))

(deftest clickhouse-tolowercase-filter-case-insensitive
  (mt/test-driver
   :clickhouse
   (is (= [[1 "Я_1"] [3 "Я_2"] [4 "Я"] [5 "я"]]
          (qp.test/formatted-rows
           [int str]
           :format-nil-values
           (data/dataset
            (tx/dataset-definition
             "metabase_tests_lowercase"
             ["test-data-lowercase"
              [{:field-name "mystring" :base-type :type/Text}]
              [["Я_1"] ["R"] ["Я_2"] ["Я"] ["я"] [nil]]])
            (data/run-mbql-query test-data-lowercase
                                 {:filter [:contains $mystring "Я"
                                           {:case-sensitive false}]})))))))


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
  (tx/dbdef->connection-details :clickhouse
                                :db
                                {:database-name "metabase_test"}))

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

(defn- do-with-metabase-test-db
  {:style/indent 0}
  [f]
  (create-metabase-test-db!)
  (tt/with-temp Database
    [database
     {:engine :clickhouse :details (metabase-test-db-details)}]
    (sync-metadata/sync-db-metadata! database)
    (f database)))

;; check that describe-table properly describes the database & base types of the enum fields
(deftest clickhouse-enums-test
  (mt/test-driver
   :clickhouse
   (is (= {:name "enums_test"
           :fields #{{:name "enum1"
                      :database-type "Enum8"
                      :base-type :type/Text
                      :database-position 0}
                     {:name "enum2"
                      :database-type "Enum16"
                      :base-type :type/Text
                      :database-position 1}}}
          (do-with-metabase-test-db
           (fn [db]
             (driver/describe-table :clickhouse db {:name "enums_test"})))))))

(deftest clickhouse-enums-test-filter
  (mt/test-driver
   :clickhouse
   (is (= [["use"]]
          (qp.test/formatted-rows
           [str]
           :format-nil-values
           (do-with-metabase-test-db
            (fn [db]
              (data/with-db db
                (data/run-mbql-query
                 enums_test
                 {:expressions {"test" [:substring $enum2 3 3]}
                  :fields [[:expression "test"]]
                  :filter [:= $enum1 "foo"]})))))))))

(deftest clickhouse-ipv4query-test
  (mt/test-driver
   :clickhouse
   (is (= [[1]]
          (qp.test/formatted-rows
           [int]
           :format-nil-values
           (do-with-metabase-test-db
            (fn [db]
              (data/with-db db
                (data/run-mbql-query
                 ipaddress_test
                 {:filter [:= $ipvfour "127.0.0.1"]
                  :aggregation [:count]})))))))))


(deftest clickhouse-basic-connection-string
  (is (= {:classname "ru.yandex.clickhouse.ClickHouseDriver"
          :subprotocol "clickhouse"
          :subname "//localhost:8123/foo?sessionTimeout=42"
          :user "default"
          :password ""
          :ssl false
          :use_server_time_zone_for_dates true}
         (sql-jdbc.conn/connection-details->spec
          :clickhouse
          {:host "localhost"
           :port "8123"
           :dbname "foo"
           :additional-options "sessionTimeout=42"}))))
