(ns metabase.driver.clickhouse
  "Driver for ClickHouse databases"
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honeysql.core :as hsql]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.driver
             [common :as driver.common]
             [sql :as sql]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.schema :as mbql.s]
            [metabase.util
             [date :as du]
             [honeysql-extensions :as hx]]
            [schema.core :as s])
  (:import [ru.yandex.clickhouse.util ClickHouseArrayUtil]
           [java.sql DatabaseMetaData ResultSet Time Types]
           [java.util Calendar Date]))

(driver/register! :clickhouse, :parent :sql-jdbc)

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [
    [#"Array"       :type/Array]
    [#"DateTime"    :type/DateTime]
    [#"Date"        :type/Date]
    [#"Decimal"     :type/Decimal]
    [#"Enum8"       :type/*]
    [#"Enum16"      :type/*]
    [#"FixedString" :type/Text]
    [#"Float32"     :type/Float]
    [#"Float64"     :type/Float]
    [#"Int8"        :type/Integer]
    [#"Int16"       :type/Integer]
    [#"Int32"       :type/Integer]
    [#"Int64"       :type/BigInteger]
    [#"String"      :type/Text]
    [#"Tuple"       :type/*]
    [#"UInt8"       :type/Integer]
    [#"UInt16"      :type/Integer]
    [#"UInt32"      :type/Integer]
    [#"UInt64"      :type/BigInteger]
    [#"UUID"        :type/UUID]]))

(defmethod sql-jdbc.sync/database-type->base-type :clickhouse [_ database-type]
  (database-type->base-type
   (str/replace (name database-type) #"(?:Nullable|LowCardinality)\((\S+)\)" "$1")))

(defmethod sql-jdbc.sync/excluded-schemas :clickhouse [_]
  #{"system"})

(defmethod sql-jdbc.conn/connection-details->spec :clickhouse
  [_ {:keys [user password dbname host port ssl]
      :or   {user "default", password "", dbname "default", host "localhost", port "8123"}
      :as   details}]
  (-> {:classname                      "ru.yandex.clickhouse.ClickHouseDriver"
       :subprotocol                    "clickhouse"
       :subname                        (str "//" host ":" port "/" dbname)
       :password                       password
       :user                           user
       :ssl                            (boolean ssl)
       :use_server_time_zone_for_dates true}
      (sql-jdbc.common/handle-additional-options details, :seperator-style :url)))

(defn- modulo [a b]
  (hsql/call :modulo a b))

(defn- to-relative-day-num [expr]
  (hsql/call :toRelativeDayNum (hsql/call :toDateTime expr)))

(defn- to-relative-month-num [expr]
  (hsql/call :toRelativeMonthNum (hsql/call :toDateTime expr)))

(defn- to-start-of-year [expr]
  (hsql/call :toStartOfYear (hsql/call :toDateTime expr)))

(defn- to-day-of-year [expr]
  (hx/+
   (hx/- (to-relative-day-num expr)
          (to-relative-day-num (to-start-of-year expr)))
   1))

(defn- to-week-of-year [expr]
  (hsql/call :toUInt8 (hsql/call :formatDateTime (hx/+ (hsql/call :toDate expr) 1) "%V")))

(defn- to-month-of-year [expr]
  (hx/+
   (hx/- (to-relative-month-num expr)
          (to-relative-month-num (to-start-of-year expr)))
   1))

(defn- to-quarter-of-year [expr]
  (hsql/call :ceil (hx//
                    (hx/+
                     (hx/- (to-relative-month-num expr)
                           (to-relative-month-num (to-start-of-year expr)))
                     1)
                    3)))

(defn- to-start-of-week [expr]
  ;; ClickHouse weeks start on Monday
  (hx/- (hsql/call :toMonday (hx/+ (hsql/call :toDate expr) 1)) 1))

(defn- to-start-of-minute [expr]
  (hsql/call :toStartOfMinute (hsql/call :toDateTime expr)))

(defn- to-start-of-hour [expr]
  (hsql/call :toStartOfHour (hsql/call :toDateTime expr)))

(defn- to-hour [expr]
  (hsql/call :toHour (hsql/call :toDateTime expr)))

(defn- to-minute [expr]
  (hsql/call :toMinute (hsql/call :toDateTime expr)))

(defn- to-day [expr]
  (hsql/call :toDate expr))

(defn- to-day-of-week [expr]
  ;; ClickHouse weeks start on Monday
  (hx/+ (modulo (hsql/call :toDayOfWeek (hsql/call :toDateTime expr)) 7) 1))

(defn- to-day-of-month [expr]
  (hsql/call :toDayOfMonth (hsql/call :toDateTime expr)))

(defn- to-start-of-month [expr]
  (hsql/call :toStartOfMonth (hsql/call :toDateTime expr)))

(defn- to-start-of-quarter [expr]
  (hsql/call :toStartOfQuarter (hsql/call :toDateTime expr)))

(defmethod sql.qp/date [:clickhouse :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:clickhouse :minute]          [_ _ expr] (to-start-of-minute expr))
(defmethod sql.qp/date [:clickhouse :minute-of-hour]  [_ _ expr] (to-minute expr))
(defmethod sql.qp/date [:clickhouse :hour]            [_ _ expr] (to-start-of-hour expr))
(defmethod sql.qp/date [:clickhouse :hour-of-day]     [_ _ expr] (to-hour expr))
(defmethod sql.qp/date [:clickhouse :day-of-week]     [_ _ expr] (to-day-of-week expr))
(defmethod sql.qp/date [:clickhouse :day-of-month]    [_ _ expr] (to-day-of-month expr))
(defmethod sql.qp/date [:clickhouse :day-of-year]     [_ _ expr] (to-day-of-year expr))
(defmethod sql.qp/date [:clickhouse :week-of-year]    [_ _ expr] (to-week-of-year expr))
(defmethod sql.qp/date [:clickhouse :month]           [_ _ expr] (to-start-of-month expr))
(defmethod sql.qp/date [:clickhouse :month-of-year]   [_ _ expr] (to-month-of-year expr))
(defmethod sql.qp/date [:clickhouse :quarter-of-year] [_ _ expr] (to-quarter-of-year expr))
(defmethod sql.qp/date [:clickhouse :year]            [_ _ expr] (to-start-of-year expr))

(defmethod sql.qp/date [:clickhouse :day]             [_ _ expr] (to-day expr))
(defmethod sql.qp/date [:clickhouse :week]            [_ _ expr] (to-start-of-week expr))
(defmethod sql.qp/date [:clickhouse :quarter]         [_ _ expr] (to-start-of-quarter expr))

(defmethod sql.qp/unix-timestamp->timestamp [:clickhouse :seconds] [_ _ expr]
  (hsql/call :toDateTime expr))

(defmethod unprepare/unprepare-value [:clickhouse Date] [_ value]
  (format "parseDateTimeBestEffort('%s')" (du/date->iso-8601 value)))

(prefer-method unprepare/unprepare-value [:clickhouse Date] [:sql Time])

;; Parameter values for date ranges are set via TimeStamp. This confuses the ClickHouse
;; server, so we override the default formatter
(s/defmethod sql/->prepared-substitution [:clickhouse java.util.Date] :- sql/PreparedStatementSubstitution
  [_ date]
  (sql/make-stmt-subs "?" [(du/format-date "yyyy-MM-dd" date)]))

;; ClickHouse doesn't support `TRUE`/`FALSE`; it uses `1`/`0`, respectively;
;; convert these booleans to numbers.
(defmethod sql.qp/->honeysql [:clickhouse Boolean]
  [_ bool]
  (if bool 1 0))

(defmethod sql.qp/->honeysql [:clickhouse :stddev]
  [driver [_ field]]
  (hsql/call :stddevSamp (sql.qp/->honeysql driver field)))

(defmethod sql.qp/->honeysql [:clickhouse :/]
  [driver args]
  (let [args (for [arg args]
               (hsql/call :toFloat64 (sql.qp/->honeysql driver arg)))]
    ((get-method sql.qp/->honeysql [:sql :/]) driver args)))

;; the filter criterion reads "is empty"
;; also see desugar.clj
(defmethod sql.qp/->honeysql [:clickhouse :=] [driver [_ field value]]
  (let [base-type (:base_type (last value)) value-value (:value value)]
    (if (and (isa? base-type :type/Text)
             (nil? value-value))
      [:or
       [:= (sql.qp/->honeysql driver field) (sql.qp/->honeysql driver value)]
       [:= (hsql/call :empty (sql.qp/->honeysql driver field)) 1]]
      ((get-method sql.qp/->honeysql [:sql :=]) driver [_ field value]))))

;; the filter criterion reads "not empty"
;; also see desugar.clj
(defmethod sql.qp/->honeysql [:clickhouse :!=] [driver [_ field value]]
  (let [base-type (:base_type (last value)) value-value (:value value)]
    (if (and (isa? base-type :type/Text)
             (nil? value-value))
      [:and
       [:!= (sql.qp/->honeysql driver field) (sql.qp/->honeysql driver value)]
       [:= (hsql/call :notEmpty (sql.qp/->honeysql driver field)) 1]]
      ((get-method sql.qp/->honeysql [:sql :!=]) driver [_ field value]))))


;; I do not know why the tests expect nil counts for empty results
;; but that's how it is :-)
;; metabase.query-processor-test.count-where-test
;; metabase.query-processor-test.share-test
(defmethod sql.qp/->honeysql [:clickhouse :count-where]
  [driver [_ pred]]
  (hsql/call :case
             (hsql/call :> (hsql/call :count) 0)
             (hsql/call :sum (hsql/call :case
                                        (sql.qp/->honeysql driver pred) 1.0
                                        :else                           0.0))
             :else nil))

(defmethod sql.qp/quote-style :clickhouse [_] :mysql)

;; The following lines make sure we call lowerUTF8 instead of lower
(defn- ch-like-clause
  [driver field value options]
  (if (get options :case-sensitive true)
    [:like field                    (sql.qp/->honeysql driver value)]
    [:like (hsql/call :lowerUTF8 field) (sql.qp/->honeysql driver (update value 1 str/lower-case))]))

(s/defn ^:private update-string-value :- mbql.s/value
  [value :- (s/constrained mbql.s/value #(string? (second %)) "string value"), f]
  (update value 1 f))

(defmethod sql.qp/->honeysql [:clickhouse :starts-with] [driver [_ field value options]]
  (ch-like-clause driver (sql.qp/->honeysql driver field) (update-string-value value #(str % \%)) options))

(defmethod sql.qp/->honeysql [:clickhouse :contains] [driver [_ field value options]]
  (ch-like-clause driver (sql.qp/->honeysql driver field) (update-string-value value #(str \% % \%)) options))

(defmethod sql.qp/->honeysql [:clickhouse :ends-with] [driver [_ field value options]]
  (ch-like-clause driver (sql.qp/->honeysql driver field) (update-string-value value #(str \% %)) options))

;; ClickHouse aliases are globally usable. Once an alias is introduced, we
;; can not refer to the same field by qualified name again, unless we mean
;; it. See https://clickhouse.yandex/docs/en/query_language/syntax/#peculiarities-of-use
;; We add a suffix to make the reference in the query unique.
(defmethod sql.qp/field->alias :clickhouse [_ field]
  (str (:name field) "_mb_alias"))

;; See above. We are removing the artificial alias suffix
(defmethod driver/execute-query :clickhouse [driver query]
  (update-in
   (sql-jdbc.execute/execute-query driver query)
   [:columns]
   (fn [columns]
     (mapv (fn [column]
             (if (str/ends-with? column "_mb_alias")
               (subs column 0 (str/last-index-of column "_mb_alias"))
               column))
           columns))))

(defmethod sql-jdbc.execute/read-column [:clickhouse Types/TIMESTAMP] [driver calendar resultset meta i]
  (when-let [timestamp (.getTimestamp resultset i)]
    (if (str/starts-with? (.toString timestamp) "1970-01-01")
      (Time. (.getTime timestamp))
      ((get-method sql-jdbc.execute/read-column [:sql-jdbc Types/TIMESTAMP]) driver calendar resultset meta i))))

(defmethod sql-jdbc.execute/read-column [:clickhouse Types/ARRAY] [driver calendar resultset meta i]
  (when-let [arr (.getArray resultset i)]
    (ClickHouseArrayUtil/arrayToString (.getArray arr))))

(defn- get-tables
  "Fetch a JDBC Metadata ResultSet of tables in the DB, optionally limited to ones belonging to a given schema."
  [^DatabaseMetaData metadata, ^String schema-or-nil, ^String db-name-or-nil]
  (vec
   (jdbc/metadata-result
    (.getTables metadata db-name-or-nil schema-or-nil "%" ; tablePattern "%" = match all tables
                (into-array String ["TABLE", "VIEW", "FOREIGN TABLE", "MATERIALIZED VIEW"])))))

(defn- post-filtered-active-tables
  [driver, ^DatabaseMetaData metadata, & [db-name-or-nil]]
  (set (for [table   (filter #(not (contains? (sql-jdbc.sync/excluded-schemas driver) (:table_schem %)))
                             (get-tables metadata db-name-or-nil nil))]
         (let [remarks (:remarks table)]
           {:name        (:table_name  table)
            :schema      (:table_schem table)
            :description (when-not (str/blank? remarks)
                           remarks)}))))

(defn- ->spec [db-or-id-or-spec]
  (if (u/id db-or-id-or-spec)
    (sql-jdbc.conn/db->pooled-connection-spec db-or-id-or-spec)
    db-or-id-or-spec))

;; ClickHouse exposes databases as schemas, but MetaBase sees
;; schemas as sub-entities of a database, at least the fast-active-tables
;; implementation would lead to duplicate tables because it iterates
;; over all schemas of the current dbs and then retrieves all
;; tables of a schema
(defmethod driver/describe-database :clickhouse
  [driver db-or-id-or-spec]
  (jdbc/with-db-metadata [metadata (->spec db-or-id-or-spec)]
    {:tables (post-filtered-active-tables
              ;; TODO: this only covers the db case, not id or spec
              driver metadata (get-in db-or-id-or-spec [:details :db]))}))

(defmethod driver.common/current-db-time-date-formatters :clickhouse [_]
  (driver.common/create-db-time-formatters "yyyy-MM-dd HH:mm.ss"))

(defmethod driver.common/current-db-time-native-query :clickhouse [_]
  "SELECT formatDateTime(NOW(), '%F %R.%S')")

(defmethod driver/current-db-time :clickhouse [& args]
  (apply driver.common/current-db-time args))



(defmethod driver/display-name :clickhouse [_] "ClickHouse")

(defmethod driver/supports? [:clickhouse :foreign-keys] [_ _] false)

;; TODO: Nested queries are actually supported, but I do not know how
;; to make the driver use correct aliases per sub-query
(defmethod driver/supports? [:clickhouse :nested-queries] [_ _] false)

(defmethod driver/date-add :clickhouse
  [_ dt amount unit]
  (hx/+ (hx/->timestamp dt) (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))
