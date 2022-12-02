(ns metabase.driver.clickhouse-test
  "Tests for specific behavior of the ClickHouse driver."
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require
   [metabase.driver.clickhouse-test-utils :as ctu]
   [clojure.test :refer :all]
   [metabase.driver :as driver]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.query-processor-test :as qp.test]
   [metabase.test :as mt]
   [metabase.test.data :as data]
   [metabase.test.data [interface :as tx]]
   [metabase.test.util :as tu]))

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

(deftest clickhouse-array-of-arrays
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array (list
                           (into-array (list "foo" "bar"))
                           (into-array (list "qaz" "qux"))))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_arrays"
                                              ["test-data-array-of-arrays"
                                               [{:field-name "my_array_of_arrays"
                                                 :base-type {:native "Array(Array(String))"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-arrays {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[['foo','bar'],['qaz','qux']]"], ["[]"]] result)))))

(deftest clickhouse-low-cardinality-array
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array (list "foo" "bar"))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_low_cardinality_array"
                                              ["test-data-low-cardinality-array"
                                               [{:field-name "my_low_card_array"
                                                 :base-type {:native "Array(LowCardinality(String))"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-low-cardinality-array {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["['foo','bar']"], ["[]"]] result)))))

(deftest clickhouse-array-of-nullables
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array (list "foo" nil "bar"))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_nullables"
                                              ["test-data-array-of-nullables"
                                               [{:field-name "my_array_of_nullables"
                                                 :base-type {:native "Array(Nullable(String))"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-nullables {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["['foo',NULL,'bar']"], ["[]"]] result)))))

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
          (ctu/do-with-metabase-test-db
           (fn [db]
             (driver/describe-table :clickhouse db {:name "enums_test"})))))))

(deftest clickhouse-enums-test-filter
  (mt/test-driver
   :clickhouse
   (is (= [["use"]]
          (qp.test/formatted-rows
           [str]
           :format-nil-values
           (ctu/do-with-metabase-test-db
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
           (ctu/do-with-metabase-test-db
            (fn [db]
              (data/with-db db
                (data/run-mbql-query
                 ipaddress_test
                 {:filter [:= $ipvfour "127.0.0.1"]
                  :aggregation [:count]})))))))))


(deftest clickhouse-basic-connection-string
  (is (= {:classname "com.clickhouse.jdbc.ClickHouseDriver"
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
