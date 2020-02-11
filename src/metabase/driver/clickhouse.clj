(ns metabase.driver.clickhouse
  "Driver for ClickHouse databases"
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honeysql.core :as hsql]
            [java-time :as t]
            [metabase
             [config :as config]
             [driver :as driver]
             [util :as u]]
            [metabase.driver.sql :as sql]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.schema :as mbql.s]
            [metabase.util
             [date-2 :as u.date]
             [honeysql-extensions :as hx]]
            [schema.core :as s])
  (:import [ru.yandex.clickhouse.util ClickHouseArrayUtil]
           [java.sql DatabaseMetaData PreparedStatement ResultSet Time Types]
           [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]
           [java.time.temporal Temporal]
           [java.util TimeZone]))

(driver/register! :clickhouse, :parent :sql-jdbc)

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [
    [#"Array"       :type/Array]
    [#"DateTime"    :type/DateTime]
    [#"Date"        :type/Date]
    [#"Decimal"     :type/Decimal]
    [#"Enum8"       :type/Enum]
    [#"Enum16"      :type/Enum]
    [#"FixedString" :type/TextLike]
    [#"Float32"     :type/Float]
    [#"Float64"     :type/Float]
    [#"Int8"        :type/Integer]
    [#"Int16"       :type/Integer]
    [#"Int32"       :type/Integer]
    [#"Int64"       :type/BigInteger]
    [#"IPv4"        :type/IPAddress]
    [#"IPv6"        :type/IPAddress]
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
  ;; ClickHouse weeks usually start on Monday
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
  ;; ClickHouse weeks usually start on Monday
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
  (format "parseDateTimeBestEffort('%s')" (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)))

(defmethod unprepare/unprepare-value [:clickhouse OffsetDateTime]
  [_ t]
  (format "parseDateTimeBestEffort('%s')" (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)))

(defmethod unprepare/unprepare-value [:clickhouse ZonedDateTime]
  [_ t]
  (format "'%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)))

;; ClickHouse doesn't support `TRUE`/`FALSE`; it uses `1`/`0`, respectively;
;; convert these booleans to UInt8
(defmethod sql.qp/->honeysql [:clickhouse Boolean]
  [_ bool]
  (if bool 1 0))

(defmethod sql/->prepared-substitution [:clickhouse Boolean]
  [driver bool]
  (sql/->prepared-substitution driver (if bool 1 0)))

;; Metabase supplies parameters for Date fields as ZonedDateTime
;; ClickHouse complains about too long parameter values. This is unfortunate
;; because it eats some performance, but I do not know a better solution
(defmethod sql.qp/->honeysql [:clickhouse ZonedDateTime]
  [driver t]
  (hsql/call :parseDateTimeBestEffort t))

(defmethod sql.qp/->honeysql [:clickhouse LocalTime]
  [driver t]
  (sql.qp/->honeysql driver (t/local-date-time
                             (t/local-date 1970 1 1)
                             t)))

(defmethod sql.qp/->honeysql [:clickhouse OffsetTime]
  [driver t]
  (sql.qp/->honeysql driver (t/offset-date-time
                             (t/local-date-time
                              (t/local-date 1970 1 1)
                              (.toLocalTime t))
                             (.getOffset t))))

(defmethod sql.qp/->honeysql [:clickhouse :stddev]
  [driver [_ field]]
  (hsql/call :stddevSamp (sql.qp/->honeysql driver field)))

(defmethod sql.qp/->honeysql [:clickhouse :count]
  [driver [_ field]]
  (if field
    (hsql/call :count (sql.qp/->honeysql driver field))
    :%count))

(defmethod sql.qp/->float :clickhouse
  [_ value]
  (hsql/call :toFloat64 value))

;; the filter criterion reads "is empty"
;; also see desugar.clj
(defmethod sql.qp/->honeysql [:clickhouse :=] [driver [_ field value]]
  (let [[qual valuevalue fieldinfo] value]
    (if (and
         (isa? qual :value)
         (isa? (:base_type fieldinfo) :type/Text)
         (nil? valuevalue))
      [:or
       [:= (sql.qp/->honeysql driver field) (sql.qp/->honeysql driver value)]
       [:= (hsql/call :empty (sql.qp/->honeysql driver field)) 1]]
      ((get-method sql.qp/->honeysql [:sql :=]) driver [_ field value]))))

;; the filter criterion reads "not empty"
;; also see desugar.clj
(defmethod sql.qp/->honeysql [:clickhouse :!=] [driver [_ field value]]
  (let [[qual valuevalue fieldinfo] value]
    (if (and
         (isa? qual :value)
         (isa? (:base_type fieldinfo) :type/Text)
         (nil? valuevalue))
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

(defmethod sql-jdbc.execute/read-column [:clickhouse Types/TIMESTAMP] [_ _ rs _ i]
  (let [r (.getObject rs i OffsetDateTime)]
    (cond
      (nil? r) nil
      (= (.toLocalDate r) (t/local-date 1970 1 1)) (.toOffsetTime r)
      :else r)))

(defmethod sql-jdbc.execute/read-column [:clickhouse Types/TIME] [_ _ rs _ i]
  (.getObject rs i OffsetTime))

;; (defmethod sql-jdbc.execute/read-column [:clickhouse Types/DATE] [_ _ rs _ i]
;;    (.getObject rs i OffsetDateTime))

(defmethod sql-jdbc.execute/read-column [:clickhouse Types/ARRAY] [driver calendar resultset meta i]
  (when-let [arr (.getArray resultset i)]
    (let [tz (if (nil? calendar) (TimeZone/getDefault) (.getTimeZone calendar))]
      (ClickHouseArrayUtil/arrayToString (.getArray arr) tz tz))))

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

(defmethod driver/describe-table :clickhouse [driver database table]
  (let [t (sql-jdbc.sync/describe-table driver database table)]
    (merge
     t
     {:fields
      (set (for [f (:fields t)]
             (update-in f [:database-type] clojure.string/replace  #"^(Enum.+)\(.+\)" "$1")))})))

(defmethod driver/display-name :clickhouse [_] "ClickHouse")

(defmethod driver/supports? [:clickhouse :standard-deviation-aggregations] [_ _] true)

(defmethod driver/db-default-timezone :clickhouse
  [_ db]
  (let [spec                             (sql-jdbc.conn/db->pooled-connection-spec db)
        sql                              (str "SELECT timezone() AS tz")
        [{:keys [tz]}] (jdbc/query spec sql)]
    tz))

;; For tests only: Get FK info via some metadata table
(defmethod driver/describe-table-fks :clickhouse
  [driver db-or-id-or-spec table & [^String db-name-or-nil]]
  (if config/is-test?
    (let [db-name (if (nil? db-name-or-nil)
                    (:name  db-or-id-or-spec)
                    db-name-or-nil)]
      (try
        (let [fks (jdbc/query
                   (->spec db-or-id-or-spec)
                   [(str/join ["SELECT * FROM `"
                               (if (str/blank? db-name) "default" db-name)
                               "-mbmeta`"])])]
          (let [myset (set
                       (for [fk fks]
                         {:fk-column-name   (:fk_source_column fk)
                          :dest-table       {:name   (:fk_dest_table fk)
                                             :schema (if (str/blank? db-name) "default" db-name)}
                          :dest-column-name (:fk_dest_column fk)}))]
            myset))
        (catch Exception e
          (driver/describe-table-fks :sql-jdbc db-or-id-or-spec table))))
    nil))

(defmethod driver/date-add :clickhouse
  [_ dt amount unit]
  (hx/+ (hx/->timestamp dt) (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))



