(ns metabase.driver.clickhouse-test
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.test :refer :all]
            [java-time.api :as t]
            [metabase.driver.clickhouse-base-types-test]
            [metabase.driver.clickhouse-temporal-bucketing-test]
            [metabase.query-processor :as qp]
            [metabase.test :as mt]
            [metabase.test.data :as data]
            [metabase.test.data [interface :as tx]]
            [metabase.test.data.clickhouse :as ctd]
            [metabase.util :as u]
            [schema.core :as s])
  (:import (java.time LocalDate LocalDateTime)))

(set! *warn-on-reflection* true)

(defn- get-mbql
  [value db]
  (let [uuid (str (java.util.UUID/randomUUID))]
    {:database (mt/id)
     :type "native"
     :native {:collection "test-table"
              :template-tags
              {:x {:id uuid
                   :name "d"
                   :display-name "D"
                   :type "dimension"
                   :dimension ["field" (mt/id :test-table :d) nil]
                   :required true}}
              :query (format "SELECT * FROM `%s`.`test_table` WHERE {{x}}" db)}
     :parameters [{:type "date/all-options"
                   :value value
                   :target ["dimension" ["template-tag" "x"]]
                   :id uuid}]}))

(def ^:private clock (t/mock-clock (t/instant "2019-11-30T23:00:00Z") (t/zone-id "UTC")))
(s/defn ^:private local-date-now      :- LocalDate     [] (LocalDate/now clock))
(s/defn ^:private local-date-time-now :- LocalDateTime [] (LocalDateTime/now clock))

(deftest clickhouse-variables-field-filters-datetime-and-datetime64
  (mt/test-driver
   :clickhouse
   (mt/with-clock clock
     (letfn
      [(->clickhouse-input
         [^LocalDateTime ldt]
         [(t/format "yyyy-MM-dd HH:mm:ss" ldt)])
       (get-test-table
         [rows native-type]
         ["test_table"
          [{:field-name "d"
            :base-type {:native native-type}}]
          (map ->clickhouse-input rows)])
       (->iso-str
         [^LocalDateTime ldt]
         (t/format "yyyy-MM-dd'T'HH:mm:ss'Z'" ldt))]
       (doseq [base-type ["DateTime" "DateTime64"]]
         (testing base-type
           (testing "on specific"
             (let [db    (format "metabase_tests_variables_replacement_on_specific_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-time-now)
                   row1  (.minusHours   now 14)
                   row2  (.minusMinutes now 20)
                   row3  (.plusMinutes  now 5)
                   row4  (.plusHours    now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "date"
                  (is (= [[(->iso-str row1)] [(->iso-str row2)] [(->iso-str row3)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "2019-11-30" db))))))
                (testing "datetime"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "2019-11-30T22:40:00" db)))))))))
           (testing "past/next minutes"
             (let [db    (format "metabase_tests_variables_replacement_past_next_minutes_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-time-now)
                   row1  (.minusHours   now 14)
                   row2  (.minusMinutes now 20)
                   row3  (.plusMinutes  now 5)
                   row4  (.plusHours    now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "past30minutes"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past30minutes" db))))))
                (testing "next30minutes"
                  (is (= [[(->iso-str row3)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next30minutes" db)))))))))
           (testing "past/next hours"
             (let [db    (format "metabase_tests_variables_replacement_past_next_hours_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-time-now)
                   row1  (.minusHours now 14)
                   row2  (.minusHours now 2)
                   row3  (.plusHours  now 25)
                   row4  (.plusHours  now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "past12hours"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past12hours" db))))))
                (testing "next12hours"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next12hours" db)))))))))
           (testing "past/next days"
             (let [db    (format "metabase_tests_variables_replacement_past_next_days_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-time-now)
                   row1  (.minusDays now 14)
                   row2  (.minusDays now 2)
                   row3  (.plusDays  now 25)
                   row4  (.plusDays  now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "past12days"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past12days" db))))))
                (testing "next12days"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next12days" db)))))))))
           (testing "past/next months/quarters"
             (let [db    (format "metabase_tests_variables_replacement_past_next_months_quarters_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-time-now)
                   row1  (.minusMonths now 14)
                   row2  (.minusMonths now 4)
                   row3  (.plusMonths  now 25)
                   row4  (.plusMonths  now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "past12months"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past12months" db))))))
                (testing "next12months"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next12months" db))))))
                (testing "past3quarters"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past3quarters" db))))))
                (testing "next3quarters"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next3quarters" db)))))))))
           (testing "past/next years"
             (let [db    (format "metabase_tests_variables_replacement_past_next_years_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-time-now)
                   row1  (.minusYears now 14)
                   row2  (.minusYears now 4)
                   row3  (.plusYears  now 25)
                   row4  (.plusYears  now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "past12years"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past12years" db))))))
                (testing "next12years"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next12years" db)))))))))))))))

(deftest clickhouse-variables-field-filters-date-and-date32
  (mt/test-driver
   :clickhouse
   (mt/with-clock clock
     (letfn
      [(->clickhouse-input
         [^LocalDate ld]
         [(t/format "yyyy-MM-dd" ld)])
       (get-test-table
         [rows native-type]
         ["test_table"
          [{:field-name "d"
            :base-type {:native native-type}}]
          (map ->clickhouse-input rows)])
       (->iso-str
         [^LocalDate ld]
         (str (t/format "yyyy-MM-dd" ld) "T00:00:00Z"))]
       (doseq [base-type ["Date" "Date32"]]
         (testing base-type
           (testing "on specific date"
             (let [db    (format "metabase_tests_variables_replacement_on_specific_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-time-now)
                   row1  (.minusDays now 14)
                   row2  now
                   row3  (.plusDays  now 25)
                   row4  (.plusDays  now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (is (= [[(->iso-str row2)]]
                       (ctd/rows-without-index (qp/process-query (get-mbql "2019-11-30" db))))))))
           (testing "past/next days"
             (let [db    (format "metabase_tests_variables_replacement_past_next_days_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-now)
                   row1  (.minusDays now 14)
                   row2  (.minusDays now 2)
                   row3  (.plusDays  now 25)
                   row4  (.plusDays  now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "past12days"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past12days" db))))))
                (testing "next12days"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next12days" db)))))))))
           (testing "past/next months/quarters"
             (let [db    (format "metabase_tests_variables_replacement_past_next_months_quarters_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-now)
                   row1  (.minusMonths now 14)
                   row2  (.minusMonths now 4)
                   row3  (.plusMonths  now 25)
                   row4  (.plusMonths  now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "past12months"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past12months" db))))))
                (testing "next12months"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next12months" db))))))
                (testing "past3quarters"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past3quarters" db))))))
                (testing "next3quarters"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next3quarters" db)))))))))
           (testing "past/next years"
             (let [db    (format "metabase_tests_variables_replacement_past_next_years_%s"
                                 (u/lower-case-en base-type))
                   now   (local-date-now)
                   row1  (.minusYears now 14)
                   row2  (.minusYears now 4)
                   row3  (.plusYears  now 25)
                   row4  (.plusYears  now 6)
                   table (get-test-table [row1 row2 row3 row4] base-type)]
               (data/dataset
                (tx/dataset-definition db table)
                (testing "past12years"
                  (is (= [[(->iso-str row2)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "past12years" db))))))
                (testing "next12years"
                  (is (= [[(->iso-str row4)]]
                         (ctd/rows-without-index (qp/process-query (get-mbql "next12years" db)))))))))))))))
