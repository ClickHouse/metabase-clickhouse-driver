(ns metabase.driver.clickhouse
  "Driver for ClickHouse databases"
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honeysql
             [core :as hsql]
             [format :as hformat]
             [helpers :as h]]
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
            [metabase.util.honeysql-extensions :as hx]
            [metabase.query-processor.store :as qp.store]
            [metabase.models.field :refer [Field]]
            [schema.core :as s])
  (:import [ru.yandex.clickhouse.util ClickHouseArrayUtil]
           [java.sql DatabaseMetaData PreparedStatement ResultSet ResultSetMetaData Time Types]
           [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]
           [java.time.temporal Temporal]
           [java.util TimeZone]
           (org.eclipse.jetty.util Fields$Field)))

(driver/register! :clickhouse, :parent :sql-jdbc)

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [
    [#"Array"       :type/Array]
    [#"DateTime"    :type/DateTime]
    [#"DateTime64"  :type/DateTime]
    [#"Date"        :type/Date]
    [#"Decimal"     :type/Decimal]
    [#"Enum8"       :type/Text]
    [#"Enum16"      :type/Text]
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

;;; ------------------------------------------ Custom HoneySQL Clause Impls ------------------------------------------

(def ^:private source-table-alias
  "Default alias for all source tables. (Not for source queries; those still use the default SQL QP alias of `source`.)"
  "t1")

;; use `source-table-alias` for the source Table, e.g. `t1.field` instead of the normal `schema.table.field`
(defmethod sql.qp/->honeysql [:clickhouse (class Field)]
  [driver field]
  (binding [sql.qp/*table-alias* (or sql.qp/*table-alias* source-table-alias)]
    ((get-method sql.qp/->honeysql [:sql-jdbc (class Field)]) driver field)))

(defmethod sql.qp/apply-top-level-clause [:clickhouse :page] [_ _ honeysql-form {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can simply use limit
      (h/limit honeysql-form items)
      ;; if we need to do an offset we have to do nesting to generate a row number and where on that
      (let [over-clause (format "row_number() OVER (%s)"
                                (first (hsql/format (select-keys honeysql-form [:order-by])
                                                    :allow-dashed-names? true
                                                    :quoting :mysql)))]
        (-> (apply h/select (map last (:select honeysql-form)))
            (h/from (h/merge-select honeysql-form [(hsql/raw over-clause) :__rownum__]))
            (h/where [:> :__rownum__ offset])
            (h/limit items))))))


(defmethod sql.qp/apply-top-level-clause [:clickhouse :source-table]
  [driver _ honeysql-form {source-table-id :source-table}]
  (let [{table-name :name, schema :schema} (qp.store/table source-table-id)]
    (h/from honeysql-form [(sql.qp/->honeysql driver (hx/identifier :table schema table-name))
                           (sql.qp/->honeysql driver (hx/identifier :table-alias source-table-alias))])))

;;; ------------------------------------------- Other Driver Method Impls --------------------------------------------

(defmethod sql-jdbc.sync/database-type->base-type :clickhouse [_ database-type]
  (database-type->base-type
   (str/replace (name database-type) #"(?:Nullable|LowCardinality)\((\S+)\)" "$1")))

(defmethod sql-jdbc.sync/excluded-schemas :clickhouse [_]
  #{"system"
    "information_schema"
    "INFORMATION_SCHEMA"})

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
      (sql-jdbc.common/handle-additional-options details, :separator-style :url)))

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
  (hsql/call :toWeek (hsql/call :toDate expr)))

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

(defmethod sql.qp/date [:clickhouse :day-of-week]
  [_ _ expr]
  (sql.qp/adjust-day-of-week :clickhouse (hsql/call :dayOfWeek expr)))

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

(defmethod sql.qp/unix-timestamp->honeysql [:clickhouse :seconds] [_ _ expr]
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
  (format "'%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)))

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

(defmethod sql.qp/->honeysql [:clickhouse LocalDateTime]
  [driver t]
  (hsql/call :parseDateTimeBestEffort t))

(defmethod sql.qp/->honeysql [:clickhouse OffsetDateTime]
  [driver t]
  (hsql/call :parseDateTimeBestEffort t))

(defmethod sql.qp/->honeysql [:clickhouse LocalDate]
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

;; we still need this for decimal versus float
(defmethod sql.qp/->honeysql [:clickhouse :/]
  [driver args]
  (let [args (for [arg args]
               (hsql/call :toFloat64 (sql.qp/->honeysql driver arg)))]
    ((get-method sql.qp/->honeysql [:sql :/]) driver args)))

;; (defmethod sql.qp/->honeysql [:clickhouse :count]
;;   [driver [_ field]]
;;   (if field
;;     (hsql/call :count (sql.qp/->honeysql driver field))
;;     :%count))

(defmethod sql.qp/->honeysql [:clickhouse :log]
  [driver [_ field]]
  (hsql/call :log10 (sql.qp/->honeysql driver field)))

(defmethod hformat/fn-handler "quantile"
  [_ field p]
  (str "quantile("
       (hformat/to-sql p)
       ")("
       (hformat/to-sql field)
       ")"))

(defmethod sql.qp/->honeysql [:clickhouse :percentile]
  [driver [_ field p]]
  (hsql/call :quantile (sql.qp/->honeysql driver field) (sql.qp/->honeysql driver p)))

(defmethod hformat/fn-handler "extract_ch"
  [_ s p]
  (str "extract(" (hformat/to-sql s) "," (hformat/to-sql p) ")"))

(defmethod sql.qp/->honeysql [:clickhouse :regex-match-first]
  [driver [_ arg pattern]]
  (hsql/call :extract_ch (sql.qp/->honeysql driver arg) pattern))

(defmethod sql.qp/->honeysql [:clickhouse :stddev]
  [driver [_ field]]
  (hsql/call :stddevPop (sql.qp/->honeysql driver field)))

;; Substring does not work for Enums, so we need to cast to String
(defmethod sql.qp/->honeysql [:clickhouse :substring]
  [driver [_ arg start length]]
  (if length
    (hsql/call :substring
               (hsql/call :toString (sql.qp/->honeysql driver arg))
               (sql.qp/->honeysql driver start)
               (sql.qp/->honeysql driver length))
    (hsql/call :substring (hsql/call :toString (sql.qp/->honeysql driver arg)) (sql.qp/->honeysql driver start))))

(defmethod sql.qp/->honeysql [:clickhouse :var]
  [driver [_ field]]
  (hsql/call :varPop (sql.qp/->honeysql driver field)))

(defmethod sql.qp/->float :clickhouse
  [_ value]
  (hsql/call :toFloat64 value))

(defmethod sql.qp/->honeysql [:clickhouse :value]
  [driver value]
  (let [[_ value {base-type :base_type}] value]
    (when (some? value)
      (condp #(isa? %2 %1) base-type
        :type/IPAddress    (hsql/call :toIPv4 value)
        (sql.qp/->honeysql driver value)))))

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
;;
;; It would even be better if we could use countIf and sumIf directly
;;
;; metabase.query-processor-test.count-where-test
;; metabase.query-processor-test.share-test
(defmethod sql.qp/->honeysql [:clickhouse :count-where]
  [driver [_ pred]]
  (hsql/call :case
             (hsql/call :> (hsql/call :count) 0)
             (hsql/call :sum (hsql/call :case
                                        (sql.qp/->honeysql driver pred) 1.0
                                        :else                           nil))
             :else nil))

(defmethod sql.qp/quote-style :clickhouse [_] :mysql)

(defmethod sql.qp/add-interval-honeysql-form :clickhouse
  [_ dt amount unit]
  (hx/+ (hx/->timestamp dt) (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

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

;; We do not have Time data types, so we cheat a little bit
(defmethod sql.qp/cast-temporal-string [:clickhouse :Coercion/ISO8601->Time]
  [_driver _special_type expr]
  (hx/->timestamp (hsql/call :parseDateTimeBestEffort (hsql/call :concat "1970-01-01T", expr))))

(defmethod sql.qp/cast-temporal-byte [:clickhouse :Coercion/ISO8601->Time]
  [_driver _special_type expr]
  (hx/->timestamp expr))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/TIMESTAMP] [_ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (fn []
    (let [r (.getObject rs i LocalDateTime)]
      (cond
        (nil? r) nil
        (= (.toLocalDate r) (t/local-date 1970 1 1)) (.toLocalTime r)
        :else r))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickhouse Types/TIME] [_ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (.getObject rs i OffsetTime))

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

(defmethod sql-jdbc.sync/db-default-timezone :clickhouse
  [_ spec]
  (let [sql            (str "SELECT timezone() AS tz")
        [{:keys [tz]}] (jdbc/query spec sql)]
    tz))

(defmethod driver/db-start-of-week :clickhouse
  [_]
  :monday
)

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
