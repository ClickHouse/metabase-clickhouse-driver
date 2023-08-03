(ns metabase.driver.clickhouse-qp
  "CLickHouse driver: QueryProcessor-related definition"
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.string :as str]
            [honey.sql :as sql]
            [java-time.api :as t]
            [metabase [util :as u]]
            [metabase.driver.clickhouse-nippy]
            [metabase.driver.sql-jdbc [execute :as sql-jdbc.execute]]
            [metabase.driver.sql.query-processor :as sql.qp :refer [add-interval-honeysql-form]]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.schema :as mbql.s]
            [metabase.mbql.util :as mbql.u]
            [metabase.util.date-2 :as u.date]
            [metabase.util.honey-sql-2 :as h2x]
            [schema.core :as s])
  (:import [com.clickhouse.data.value ClickHouseArrayValue]
           [java.sql ResultSet ResultSetMetaData Types]
           [java.time
            LocalDate
            LocalDateTime
            LocalTime
            OffsetDateTime
            OffsetTime
            ZonedDateTime]
           java.util.Arrays))

;; (set! *warn-on-reflection* true)

(defmethod sql.qp/quote-style       :clickhouse [_] :mysql)
(defmethod sql.qp/honey-sql-version :clickhouse [_] 2)

(defn- clickhouse-datetime-fn
  [fn-name expr]
  [fn-name (h2x/->datetime expr)])

(defmethod sql.qp/date [:clickhouse :day-of-week]
  [_ _ expr]
  ;; a tick in the function name prevents HSQL2 to make the function call UPPERCASE
  ;; https://cljdoc.org/d/com.github.seancorfield/honeysql/2.4.1011/doc/getting-started/other-databases#clickhouse
  (sql.qp/adjust-day-of-week :clickhouse [:'dayOfWeek expr]))

(defmethod sql.qp/date [:clickhouse :default]
  [_ _ expr]
  expr)

(defmethod sql.qp/date [:clickhouse :minute]
  [_ _ expr]
  (clickhouse-datetime-fn :'toStartOfMinute expr))

(defmethod sql.qp/date [:clickhouse :minute-of-hour]
  [_ _ expr]
  (clickhouse-datetime-fn :'toMinute expr))

(defmethod sql.qp/date [:clickhouse :hour]
  [_ _ expr]
  (clickhouse-datetime-fn :'toStartOfHour expr))

(defmethod sql.qp/date [:clickhouse :hour-of-day]
  [_ _ expr]
  (clickhouse-datetime-fn :'toHour expr))

(defmethod sql.qp/date [:clickhouse :day-of-month]
  [_ _ expr]
  (clickhouse-datetime-fn :'toDayOfMonth expr))

(defn- to-start-of-week
  [expr]
  ;; ClickHouse weeks usually start on Monday
  (clickhouse-datetime-fn :'toMonday expr))

(defn- to-start-of-year
  [expr]
  (clickhouse-datetime-fn :'toStartOfYear expr))

(defn- to-relative-day-num
  [expr]
  (clickhouse-datetime-fn :'toRelativeDayNum expr))

(defn- to-day-of-year
  [expr]
  (h2x/+ (h2x/- (to-relative-day-num expr)
                (to-relative-day-num (to-start-of-year expr)))
         1))

(defmethod sql.qp/date [:clickhouse :day-of-year]
  [_ _ expr]
  (to-day-of-year expr))

(defmethod sql.qp/date [:clickhouse :week-of-year-iso]
  [_ _ expr]
  (clickhouse-datetime-fn :'toISOWeek expr))

(defmethod sql.qp/date [:clickhouse :month]
  [_ _ expr]
  (clickhouse-datetime-fn :'toStartOfMonth expr))

(defmethod sql.qp/date [:clickhouse :month-of-year]
  [_ _ expr]
  (clickhouse-datetime-fn :'toMonth expr))

(defmethod sql.qp/date [:clickhouse :quarter-of-year]
  [_ _ expr]
  (clickhouse-datetime-fn :'toQuarter expr))

(defmethod sql.qp/date [:clickhouse :year]
  [_ _ expr]
  (clickhouse-datetime-fn :'toStartOfYear expr))

(defmethod sql.qp/date [:clickhouse :day]
  [_ _ expr]
  (h2x/->date expr))

(defmethod sql.qp/date [:clickhouse :week]
  [driver _ expr]
  (sql.qp/adjust-start-of-week driver to-start-of-week expr))

(defmethod sql.qp/date [:clickhouse :quarter]
  [_ _ expr]
  (clickhouse-datetime-fn :'toStartOfQuarter expr))

(defmethod sql.qp/unix-timestamp->honeysql [:clickhouse :seconds]
  [_ _ expr]
  (h2x/->datetime expr))

(defmethod sql.qp/unix-timestamp->honeysql [:clickhouse :milliseconds]
  [_ _ expr]
  [:'toDateTime64 (h2x// expr 1000) 3])

(defn- date-time-parse-fn
  [nano]
  (if (zero? nano) :'parseDateTimeBestEffort :'parseDateTime64BestEffort))

(defmethod sql.qp/->honeysql [:clickhouse LocalDateTime]
  [_ ^java.time.LocalDateTime t]
  (let [formatted (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
        fn (date-time-parse-fn (.getNano t))]
    [fn formatted]))

(defmethod sql.qp/->honeysql [:clickhouse ZonedDateTime]
  [_ ^java.time.ZonedDateTime t]
  (let [formatted (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)
        fn (date-time-parse-fn (.getNano t))]
    [fn formatted]))

(defmethod sql.qp/->honeysql [:clickhouse OffsetDateTime]
  [_ ^java.time.OffsetDateTime t]
  ;; copy-paste due to reflection warnings
  (let [formatted (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)
        fn (date-time-parse-fn (.getNano t))]
    [fn formatted]))

(defmethod sql.qp/->honeysql [:clickhouse LocalDate]
  [_ ^java.time.LocalDate t]
  [:'parseDateTimeBestEffort t])

(defmethod sql.qp/->honeysql [:clickhouse LocalTime]
  [driver ^java.time.LocalTime t]
  (sql.qp/->honeysql driver (t/local-date-time (t/local-date 1970 1 1) t)))

(defmethod sql.qp/->honeysql [:clickhouse OffsetTime]
  [driver ^java.time.OffsetTime t]
  (sql.qp/->honeysql driver
                     (t/offset-date-time (t/local-date-time
                                          (t/local-date 1970 1 1)
                                          (.toLocalTime t))
                                         (.getOffset t))))

;; We still need this for the tests that use multiple case statements where
;; we can have either Int or Float in different branches,
;; so we just coerce everything to Float64.
;;
;; See metabase.query-processor-test.expressions-test "Can use expressions as values"
;; (defmethod sql.qp/->honeysql [:clickhouse :/]
;;   [driver args]
;;   (println args)
;;   (interpose :/ (for [arg args] [:'toFloat64 (sql.qp/->honeysql driver arg)])))

(defn- interval? [expr]
  (mbql.u/is-clause? :interval expr))

(defmethod sql.qp/->honeysql [:clickhouse :+]
  [driver [_ args]]
  (println args)
  (if (some interval? args)
    (if-let [[field intervals] (u/pick-first (complement interval?) args)]
      (reduce (fn [hsql-form [_ amount unit]]
                (add-interval-honeysql-form driver hsql-form amount unit))
              (sql.qp/->honeysql driver field)
              intervals)
      (throw (ex-info "Summing intervals is not supported" {:args args})))
    (interpose :+ (map (fn [arg] [:'toFloat64 (sql.qp/->honeysql driver arg)]) args))))

(defmethod sql.qp/->honeysql [:clickhouse :log]
  [driver [_ field]]
  [:log10 (sql.qp/->honeysql driver field)])

(defn- format-quantile
  [_fn [p field]]
  (let [[x-sql & x-args] (sql/format-expr p     {:nested true})
        [y-sql & y-args] (sql/format-expr field {:nested true})]
    (into [(format "quantile(%s)(%s)" x-sql y-sql)]
          cat [x-args y-args])))

(defn- format-extract
  [_fn [s p]]
  (let [[x-sql & x-args] (sql/format-expr s {:nested true})
        [y-sql & y-args] (sql/format-expr p {:nested true})]
    (into [(format "extract(%s,%s)" x-sql y-sql)]
          cat [x-args y-args])))

(sql/register-fn! ::quantile #'format-quantile)
(sql/register-fn! ::extract  #'format-extract)

(defmethod sql.qp/->honeysql [:clickhouse :percentile]
  [driver [_ field p]]
  [:'quantile
   (sql.qp/->honeysql driver field)
   (sql.qp/->honeysql driver p)])

(defmethod sql.qp/->honeysql [:clickhouse :regex-match-first]
  [driver [_ arg pattern]]
  [:'extract (sql.qp/->honeysql driver arg) pattern])

(defmethod sql.qp/->honeysql [:clickhouse :stddev]
  [driver [_ field]]
  [:'stddevPop (sql.qp/->honeysql driver field)])

;; Substring does not work for Enums, so we need to cast to String
(defmethod sql.qp/->honeysql [:clickhouse :substring]
  [driver [_ arg start length]]
  (let [str [:'toString (sql.qp/->honeysql driver arg)]]
    (if length
      [:'substring str
       (sql.qp/->honeysql driver start)
       (sql.qp/->honeysql driver length)]
      [:'substring str
       (sql.qp/->honeysql driver start)])))

(defmethod sql.qp/->honeysql [:clickhouse :var]
  [driver [_ field]]
  [:'varPop (sql.qp/->honeysql driver field)])

(defmethod sql.qp/->float :clickhouse
  [_ value]
  [:'toFloat64 value])

(defmethod sql.qp/->honeysql [:clickhouse :value]
  [driver value]
  (let [[_ value {base-type :base_type}] value]
    (when (some? value)
      (condp #(isa? %2 %1) base-type
        :type/IPAddress [:'toIPv4 value]
        (sql.qp/->honeysql driver value)))))

;; the filter criterion reads "is empty"
;; also see desugar.clj
(defmethod sql.qp/->honeysql [:clickhouse :=]
  [driver [_ field value]]
  (let [[qual valuevalue fieldinfo] value]
    (if (and (isa? qual :value)
             (isa? (:base_type fieldinfo) :type/Text)
             (nil? valuevalue))
      [:or
       [:= (sql.qp/->honeysql driver field) (sql.qp/->honeysql driver value)]
       [:= [:'empty (sql.qp/->honeysql driver field)] 1]]
      ((get-method sql.qp/->honeysql [:sql :=]) driver [_ field value]))))

;; the filter criterion reads "not empty"
;; also see desugar.clj
(defmethod sql.qp/->honeysql [:clickhouse :!=]
  [driver [_ field value]]
  (let [[qual valuevalue fieldinfo] value]
    (if (and (isa? qual :value)
             (isa? (:base_type fieldinfo) :type/Text)
             (nil? valuevalue))
      [:and
       [:!= (sql.qp/->honeysql driver field) (sql.qp/->honeysql driver value)]
       [:= [:'notEmpty (sql.qp/->honeysql driver field)] 1]]
      ((get-method sql.qp/->honeysql [:sql :!=]) driver [_ field value]))))

;; I do not know why the tests expect nil counts for empty results
;; but that's how it is :-)
;;
;; It would even be better if we could use countIf and sumIf directly
;;
;; metabase.query-processor-test.count-where-test
;; metabase.query-processor-test.share-test
(defmethod sql.qp/->honeysql [:clickhouse :count-where]
  [driver [_ pred]]
  [:case
   [:> [:'count] 0]
   [:sum [:case (sql.qp/->honeysql driver pred) 1 :else 0]]
   :else nil])

(defmethod sql.qp/->honeysql [:clickhouse :sum-where]
  [driver [_ field pred]]
  [:sum [:case (sql.qp/->honeysql driver pred) (sql.qp/->honeysql driver field)
         :else 0]])

(defmethod sql.qp/add-interval-honeysql-form :clickhouse
  [_ dt amount unit]
  (h2x/+ (h2x/->timestamp dt)
         [:raw (format "INTERVAL %d %s" (int amount) (name unit))]))

;; The following lines make sure we call lowerUTF8 instead of lower
(defn- ch-like-clause
  [driver field value options]
  (if (get options :case-sensitive true)
    [:like field (sql.qp/->honeysql driver value)]
    [:like [:'lowerUTF8 field]
     (sql.qp/->honeysql driver (update value 1 metabase.util/lower-case-en))]))

(s/defn ^:private update-string-value :- mbql.s/value
  [value :- (s/constrained mbql.s/value #(string? (second %)) ":string value") f]
  (update value 1 f))

(defmethod sql.qp/->honeysql [:clickhouse :contains]
  [driver [_ field value options]]
  (ch-like-clause driver
                  (sql.qp/->honeysql driver field)
                  (update-string-value value #(str \% % \%))
                  options))

(defn- clickhouse-string-fn
  [fn-name field value options]
  (let [field (sql.qp/->honeysql :clickhouse field)
        value (sql.qp/->honeysql :clickhouse value)]
    (if (get options :case-sensitive true)
      [fn-name field value]
      [[:'lowerUTF8 field] (metabase.util/lower-case-en value)])))

(defmethod sql.qp/->honeysql [:clickhouse :starts-with]
  [_ [_ field value options]]
  (clickhouse-string-fn :'startsWith field value options))

(defmethod sql.qp/->honeysql [:clickhouse :ends-with]
  [_ [_ field value options]]
  (clickhouse-string-fn :'endsWith field value options))

;; We do not have Time data types, so we cheat a little bit
(defmethod sql.qp/cast-temporal-string [:clickhouse :Coercion/ISO8601->Time]
  [_driver _special_type expr]
  (h2x/->timestamp [:'parseDateTimeBestEffort [:'concat "1970-01-01T" expr]]))

(defmethod sql.qp/cast-temporal-byte [:clickhouse :Coercion/ISO8601->Time]
  [_driver _special_type expr]
  (h2x/->timestamp expr))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/TINYINT]
  [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
  (fn []
    (.getByte rs i)))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/SMALLINT]
  [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
  (fn []
    (.getShort rs i)))

;; This is for tests only - some of them expect nil values
;; getInt/getLong return 0 in case of a NULL value in the result set
;; the only way to check if it was actually NULL - call ResultSet.wasNull afterwards
(defn- with-null-check
  [rs get-value-fn]
  (let [value (get-value-fn)]
    (if (.wasNull rs) nil value)))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/BIGINT]
  [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
  (fn []
    (with-null-check rs #(.getLong rs i))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/INTEGER]
  [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
  (fn []
    (with-null-check rs #(.getInt rs i))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/TIMESTAMP]
  [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
  (fn []
    (let [r (.getObject rs i LocalDateTime)]
      (cond (nil? r) nil
            (= (.toLocalDate r) (t/local-date 1970 1 1)) (.toLocalTime r)
            :else r))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/TIMESTAMP_WITH_TIMEZONE]
  [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
  (fn []
    (when-let [s (.getString rs i)]
      (u.date/parse s))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/TIME]
  [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
  (fn []
    (.getObject rs i OffsetTime)))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/NUMERIC]
  [_ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (fn []
    ; count is NUMERIC cause UInt64 is too large for the canonical SQL BIGINT,
    ; and defaults to BigDecimal, but we want it to be coerced to java Long
    ; cause it still fits and the tests are expecting that
    (if (= (.getColumnLabel rsmeta i) "count")
      (.getLong rs i)
      (.getBigDecimal rs i))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/ARRAY]
  [_ ^ResultSet rs ^ResultSetMetaData _ ^Integer i]
  (fn []
    (when-let [arr (.getArray rs i)]
      (let [inner (.getArray arr)]
        (cond
          ;; Booleans are returned as just bytes
          (bytes? inner)
          (str "[" (str/join ", " (map #(if (= 1 %) "true" "false") inner)) "]")
          ;; All other primitives
          (.isPrimitive (.getComponentType (.getClass inner)))
          (Arrays/toString inner)
          ;; Complex types
          :else
          (.asString (ClickHouseArrayValue/of inner)))))))

(defmethod unprepare/unprepare-value [:clickhouse LocalDate]
  [_ t]
  (format "toDate('%s')" (t/format "yyyy-MM-dd" t)))

(defmethod unprepare/unprepare-value [:clickhouse LocalTime]
  [_ t]
  (format "'%s'" (t/format "HH:mm:ss.SSS" t)))

(defmethod unprepare/unprepare-value [:clickhouse OffsetTime]
  [_ t]
  (format "'%s'" (t/format "HH:mm:ss.SSSZZZZZ" t)))

(defmethod unprepare/unprepare-value [:clickhouse LocalDateTime]
  [_ t]
  (format "'%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)))

(defmethod unprepare/unprepare-value [:clickhouse OffsetDateTime]
  [_ t]
  (format "%s('%s')"
          (if (zero? (.getNano t)) "parseDateTimeBestEffort" "parseDateTime64BestEffort")
          (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)))

(defmethod unprepare/unprepare-value [:clickhouse ZonedDateTime]
  [_ t]
  (format "'%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)))
