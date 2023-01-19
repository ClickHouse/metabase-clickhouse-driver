(ns metabase.driver.clickhouse-test
  "Tests for specific behavior of the ClickHouse driver."
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [cljc.java-time.format.date-time-formatter :as date-time-formatter]
            [cljc.java-time.offset-date-time :as offset-date-time]
            [cljc.java-time.temporal.chrono-unit :as chrono-unit]
            [cljc.java-time.local-date :as local-date]
            [clojure.test :refer :all]
            [metabase.driver :as driver]
            [metabase.driver.clickhouse-test-utils :as ctu]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.query-processor :as qp]
            [metabase.query-processor-test :as qp.test]
            [metabase.test :as mt]
            [metabase.test.data :as data]
            [metabase.test.data [interface :as tx]]
            [metabase.test.util :as tu]))

(deftest clickhouse-server-timezone
  (mt/test-driver
   :clickhouse
   (is (= "UTC"
          (let [spec (sql-jdbc.conn/connection-details->spec :clickhouse {})]
            (metabase.driver/db-default-timezone :clickhouse spec))))))

(deftest now-converted-to-timezone
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
    (= "[foo, bar]"
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
    (= "[23, 42]"
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
     (is (= [["[[foo, bar], [qaz, qux]]"], ["[]"]] result)))))

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
     (is (= [["[foo, bar]"], ["[]"]] result)))))

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
     (is (= [["[foo, null, bar]"], ["[]"]] result)))))

(deftest clickhouse-array-of-booleans
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array (list true false true))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_booleans"
                                              ["test-data-array-of-booleans"
                                               [{:field-name "my_array_of_booleans"
                                                 :base-type {:native "Array(Boolean)"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-booleans {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[true, false, true]"], ["[]"]] result)))))

(deftest clickhouse-array-of-floats
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array (list 1.2 3.4))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_floats"
                                              ["test-data-array-of-floats"
                                               [{:field-name "my_array_of_floats"
                                                 :base-type {:native "Array(Float64)"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-floats {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[1.2, 3.4]"], ["[]"]] result)))))

(deftest clickhouse-array-of-dates
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array
               (list
                (local-date/parse "2022-12-06")
                (local-date/parse "2021-10-19")))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_dates"
                                              ["test-data-array-of-dates"
                                               [{:field-name "my_array_of_dates"
                                                 :base-type {:native "Array(Date)"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-dates {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[2022-12-06, 2021-10-19]"], ["[]"]] result)))))

(deftest clickhouse-array-of-date32
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array
               (list
                (local-date/parse "2122-12-06")
                (local-date/parse "2099-10-19")))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_date32"
                                              ["test-data-array-of-date32"
                                               [{:field-name "my_array_of_date32"
                                                 :base-type {:native "Array(Date32)"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-date32 {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[2122-12-06, 2099-10-19]"], ["[]"]] result)))))

(deftest clickhouse-array-of-datetime
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array
               (list
                (offset-date-time/parse "2022-12-06T18:28:31Z")
                (offset-date-time/parse "2021-10-19T13:12:44Z")))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_datetime"
                                              ["test-data-array-of-datetime"
                                               [{:field-name "my_array_of_datetime"
                                                 :base-type {:native "Array(DateTime)"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-datetime {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[2022-12-06T18:28:31, 2021-10-19T13:12:44]"], ["[]"]] result)))))

(deftest clickhouse-array-of-datetime64
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array
               (list
                (offset-date-time/parse "2022-12-06T18:28:31.123Z")
                (offset-date-time/parse "2021-10-19T13:12:44.456Z")))
         row2 (into-array nil)
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_datetime64"
                                              ["test-data-array-of-datetime64"
                                               [{:field-name "my_array_of_datetime64"
                                                 :base-type {:native "Array(DateTime64(3))"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-datetime64 {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[2022-12-06T18:28:31.123, 2021-10-19T13:12:44.456]"], ["[]"]] result)))))

(deftest clickhouse-array-of-decimals
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array (list "12345123.123456789" "78.245"))
         row2 nil
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_decimals"
                                              ["test-data-array-of-decimals"
                                               [{:field-name "my_array_of_decimals"
                                                 :base-type {:native "Array(Decimal(18, 9))"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-decimals {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[12345123.123456789, 78.245000000]"], ["[]"]] result)))))

(deftest clickhouse-array-of-tuples
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array (list (list "foobar" 1234) (list "qaz" 0)))
         row2 nil
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_tuples"
                                              ["test-data-array-of-tuples"
                                               [{:field-name "my_array_of_tuples"
                                                 :base-type {:native "Array(Tuple(String, UInt32))"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-tuples {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[[foobar, 1234], [qaz, 0]]"], ["[]"]] result)))))

(deftest clickhouse-array-of-uuids
  (mt/test-driver
   :clickhouse
   (let [row1 (into-array (list "2eac427e-7596-11ed-a1eb-0242ac120002"
                                "2eac44f4-7596-11ed-a1eb-0242ac120002"))
         row2 nil
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_array_of_uuids"
                                              ["test-data-array-of-uuids"
                                               [{:field-name "my_array_of_uuids"
                                                 :base-type {:native "Array(UUID)"}}]
                                               [[row1] [row2]]])
                       (data/run-mbql-query test-data-array-of-uuids {}))
         result (ctu/rows-without-index query-result)]
     (is (= [["[2eac427e-7596-11ed-a1eb-0242ac120002, 2eac44f4-7596-11ed-a1eb-0242ac120002]"], ["[]"]] result)))))

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

(deftest clickhouse-booleans
  (mt/test-driver
   :clickhouse
   (let [[row1 row2 row3 row4] [["#1" true] ["#2" false] ["#3" false] ["#4" true]]
         query-result (data/dataset
                       (tx/dataset-definition "metabase_tests_booleans"
                                              ["test-data-booleans"
                                               [{:field-name "name"
                                                 :base-type :type/Text}
                                                {:field-name "is_active"
                                                 :base-type :type/Boolean}]
                                               [row1 row2 row3 row4]])
                       (data/run-mbql-query test-data-booleans {:filter [:= $is_active false]}))
         rows (qp.test/rows query-result)
         result (map #(drop 1 %) rows)] ; remove db "index" which is the first column in the result set
     (is (= [row2 row3] result)))))

(deftest clickhouse-enums-schema-test
  (mt/test-driver
   :clickhouse
   (is (= {:name "enums_test"
           :fields #{{:name "enum1"
                      :database-type "Enum8"
                      :base-type :type/Text
                      :database-position 0
                      ; TODO: in Metabase 0.45.0-RC this returned true,
                      ; and now it is false, which is strange, cause it is not Nullable in the DDL
                      :database-required false}
                     {:name "enum2"
                      :database-type "Enum16"
                      :base-type :type/Text
                      :database-position 1
                      :database-required false}
                     {:name "enum3"
                      :database-type "Enum8"
                      :base-type :type/Text
                      :database-position 2
                      :database-required false}}}
          (ctu/do-with-metabase-test-db
           (fn [db]
             (driver/describe-table :clickhouse db {:name "enums_test"})))))))

(deftest clickhouse-enums-values-test
  (mt/test-driver
   :clickhouse
   (is (= [["foo" "house" "qaz"]
           ["foo bar" "click" "qux"]
           ["bar" "house" "qaz"]]
          (qp.test/formatted-rows
           [str str str]
           :format-nil-values
           (ctu/do-with-metabase-test-db
            (fn [db]
              (data/with-db db
                (data/run-mbql-query
                 enums_test
                 {})))))))))

(deftest clickhouse-enums-test-filter
  (mt/test-driver
   :clickhouse
   (is (= [["useqa"]]
          (qp.test/formatted-rows
           [str]
           :format-nil-values
           (ctu/do-with-metabase-test-db
            (fn [db]
              (data/with-db db
                (data/run-mbql-query
                 enums_test
                 {:expressions {"test" [:concat
                                        [:substring $enum2 3 3]
                                        [:substring $enum3 1 2]]}
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
          :use_no_proxy false
          :use_server_time_zone_for_dates true}
         (sql-jdbc.conn/connection-details->spec
          :clickhouse
          {:host "localhost"
           :port "8123"
           :dbname "foo"
           :additional-options "sessionTimeout=42"}))))

(deftest clickhouse-use-no-proxy-connection-string
  (is (= {:classname "com.clickhouse.jdbc.ClickHouseDriver"
          :subprotocol "clickhouse"
          :subname "//localhost:8123/foo?sessionTimeout=42"
          :user "default"
          :password ""
          :ssl false
          :use_no_proxy true
          :use_server_time_zone_for_dates true}
         (sql-jdbc.conn/connection-details->spec
          :clickhouse
          {:host "localhost"
           :port "8123"
           :dbname "foo"
           :use-no-proxy true
           :additional-options "sessionTimeout=42"}))))
