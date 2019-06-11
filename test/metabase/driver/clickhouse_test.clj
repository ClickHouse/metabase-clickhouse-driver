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
  (-> (data/with-db-for-dataset
    [_
     (tx/dataset-definition "ClickHouse with Decimal Field"
       ["test-data"
        [{:field-name "my_money", :base-type {:native "Decimal(12,3)"}}]
        [[1.0] [23.0] [42.0] [42.0]]])]
    (data/run-mbql-query test-data
                         {:expressions {:divided [:/ $my_money 2]}
                          :filter      [:> [:expression :divided] 1.0]
                          :breakout    [[:expression :divided]]
                          :order-by    [[:desc [:expression :divided]]]
                          :limit       1}))
      first-row last float))

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
