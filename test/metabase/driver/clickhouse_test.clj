(ns metabase.driver.clickhouse-test
  "Tests for specific behavior of the ClickHouse driver."
  (:require [expectations :refer [expect]]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.util :as u]
            [metabase.query-processor-test :refer :all]
            [metabase.test.data :as data]
            [metabase.test.data
             [datasets :as datasets]
             [interface :as tx]]
            [metabase.test.util :as tu]))

(datasets/expect-with-driver :clickhouse
  "UTC"
  (tu/db-timezone-id))

(datasets/expect-with-driver :clickhouse
  21.0
  (-> (data/dataset (tx/dataset-definition "metabase_tests_decimal"
                      ["test-data-decimal"
                       [{:field-name "my_money", :base-type {:native "Decimal(12,3)"}}]
                       [[1.0] [23.0] [42.0] [42.0]]])
    (data/run-mbql-query test-data-decimal
                         {:expressions {:divided [:/ $my_money 2]}
                          :filter      [:> [:expression :divided] 1.0]
                          :breakout    [[:expression :divided]]
                          :order-by    [[:desc [:expression :divided]]]
                          :limit       1}))
      first-row last float))

(datasets/expect-with-driver :clickhouse
  "['foo','bar']"
  (-> (data/dataset (tx/dataset-definition "metabase_tests_array_string"
                      ["test-data-array-string"
                       [{:field-name "my_array", :base-type {:native "Array(String)"}}]
                       [[(into-array (list "foo" "bar"))]]])
    (data/run-mbql-query test-data-array-string {:limit 1}))
      first-row last))

(datasets/expect-with-driver :clickhouse
  "[23,42]"
  (-> (data/dataset (tx/dataset-definition "metabase_tests_array_uint64"
                      ["test-data-array-uint64"
                       [{:field-name "my_array", :base-type {:native "Array(UInt64)"}}]
                       [[(into-array (list 23 42))]]])
    (data/run-mbql-query test-data-array-uint64 {:limit 1}))
      first-row last))

(datasets/expect-with-driver :clickhouse
  2
  (-> (data/dataset (tx/dataset-definition "metabase_tests_nullable_strings"
                      ["test-data-nullable-strings"
                       [{:field-name "mystring", :base-type :type/Text}]
                       [["foo"], ["bar"], ["   "], [""], [nil]]])
                    (data/run-mbql-query test-data-nullable-strings
                                         {:filter [:is-null $mystring]
                                          :aggregation [:count]}))
      first-row last))

(datasets/expect-with-driver :clickhouse
  3
  (-> (data/dataset (tx/dataset-definition "metabase_tests_nullable_strings"
                      ["test-data-nullable-strings"
                       [{:field-name "mystring", :base-type :type/Text}]
                       [["foo"], ["bar"], ["   "], [""], [nil]]])
                    (data/run-mbql-query test-data-nullable-strings
                                         {:filter [:not-null $mystring]
                                          :aggregation [:count]}))
      first-row last))

(expect
  {:classname                      "ru.yandex.clickhouse.ClickHouseDriver"
   :subprotocol                    "clickhouse"
   :subname                        "//localhost:8123/foo?sessionTimeout=42"
   :user                           "default"
   :password                       ""
   :ssl                            false
   :use_server_time_zone_for_dates true}
  (sql-jdbc.conn/connection-details->spec :clickhouse
    {:host               "localhost"
     :port               "8123"
     :dbname             "foo"
     :additional-options "sessionTimeout=42"}))
