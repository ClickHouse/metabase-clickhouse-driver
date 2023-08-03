(ns metabase.driver.clickhouse
  "Driver for ClickHouse databases"
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [metabase [config :as config] [driver :as driver] [util :as u]]
            [metabase.driver.clickhouse-qp]
            [metabase.driver.clickhouse-nippy]
            [metabase.driver.ddl.interface :as ddl.i]
            [metabase.driver.sql :as driver.sql]
            [metabase.driver.sql-jdbc [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [sync :as sql-jdbc.sync]]))

(driver/register! :clickhouse :parent :sql-jdbc)

(def ^:private database-type->base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
   [[#"Array" :type/Array]
    [#"Bool" :type/Boolean]
    ;; TODO: test it with :type/DateTimeWithTZ
    [#"DateTime64" :type/DateTime]
    [#"DateTime" :type/DateTime]
    [#"Date" :type/Date]
    [#"Date32" :type/Date]
    [#"Decimal" :type/Decimal]
    [#"Enum8" :type/Text]
    [#"Enum16" :type/Text]
    [#"FixedString" :type/TextLike]
    [#"Float32" :type/Float]
    [#"Float64" :type/Float]
    [#"Int8" :type/Integer]
    [#"Int16" :type/Integer]
    [#"Int32" :type/Integer]
    [#"Int64" :type/BigInteger]
    [#"IPv4" :type/IPAddress]
    [#"IPv6" :type/IPAddress]
    [#"Map" :type/Dictionary]
    [#"String" :type/Text]
    [#"Tuple" :type/*]
    [#"UInt8" :type/Integer]
    [#"UInt16" :type/Integer]
    [#"UInt32" :type/Integer]
    [#"UInt64" :type/BigInteger]
    [#"UUID" :type/UUID]]))

;; Enum8(UInt8) -> Enum8, DateTime64(Europe/Amsterdam) -> DateTime64,
;; Nullable(DateTime) -> DateTime, SimpleAggregateFunction(sum, Int64) -> Int64, etc
(defn- ^:private normalize-database-type
  [database-type]
  (let [db-type    (subs (str database-type) 1) ;; keyword->str; `name` call does not work well
        normalized (second (re-find #"(?:Nullable\(|LowCardinality\()?(\w+)?\({0,1}.*" db-type))]
    ;; slightly different normalization for SimpleAggregateFunction - we need to take the second arg
    (or (keyword (if (= normalized "SimpleAggregateFunction")
                   (second (re-find #"SimpleAggregateFunction\(\w+?, {0,1}(.+)?\)" db-type))
                   normalized))
        database-type))) ;; basically, fall back to :type/* later

(defmethod sql-jdbc.sync/database-type->base-type :clickhouse
  [_ database-type]
  (database-type->base-type (normalize-database-type database-type)))

(def ^:private excluded-schemas #{"system" "information_schema" "INFORMATION_SCHEMA"})
(defmethod sql-jdbc.sync/excluded-schemas :clickhouse [_] excluded-schemas)

(def ^:private default-connection-details
  {:user "default", :password "", :dbname "default", :host "localhost", :port "8123"})
(def ^:private product-name "metabase/1.2.0")

(defmethod sql-jdbc.conn/connection-details->spec :clickhouse
  [_ details]
  ;; ensure defaults merge on top of nils
  (let [details (reduce-kv (fn [m k v] (assoc m k (or v (k default-connection-details))))
                           default-connection-details
                           details)
        {:keys [user password dbname host port ssl use-no-proxy]} details]
    (->
     {:classname "com.clickhouse.jdbc.ClickHouseDriver"
      :subprotocol "clickhouse"
      :subname (str "//" host ":" port "/" dbname)
      :password (or password "")
      :user user
      :ssl (boolean ssl)
      :use_no_proxy (boolean use-no-proxy)
      :use_server_time_zone_for_dates true
      :product_name product-name}
     (sql-jdbc.common/handle-additional-options details :separator-style :url))))

(def ^:private allowed-table-types
  (into-array String
              ["TABLE" "VIEW" "FOREIGN TABLE" "REMOTE TABLE" "DICTIONARY"
               "MATERIALIZED VIEW" "MEMORY TABLE" "LOG TABLE"]))

(defn- tables-set
  [tables]
  (set
   (for [table tables]
     (let [remarks (:remarks table)]
       {:name (:table_name table)
        :schema (:table_schem table)
        :description (when-not (str/blank? remarks) remarks)}))))

(defn- get-tables-from-metadata
  [metadata schema-pattern]
  (.getTables metadata
              nil            ; catalog - unused in the source code there
              schema-pattern
              "%"            ; tablePattern "%" = match all tables
              allowed-table-types))

(defn ^:private not-inner-mv-table?
  [table]
  (not (str/starts-with? (:table_name table) ".inner")))

(defn- ->spec
  [db]
  (if (u/id db)
    (sql-jdbc.conn/db->pooled-connection-spec db) db))

(defn- get-all-tables
  [db]
  (jdbc/with-db-metadata [metadata (->spec db)]
    (->> (get-tables-from-metadata metadata "%")
         (jdbc/metadata-result)
         (vec)
         (filter #(and
                   (not (contains? excluded-schemas (:table_schem %)))
                   (not-inner-mv-table? %)))
         (tables-set))))

;; Strangely enough, the tests only work with :db keyword,
;; but the actual sync from the UI uses :dbname
(defn- get-db-name
  [db]
  (or (get-in db [:details :dbname])
      (get-in db [:details :db])))

(defn- get-tables-in-dbs [db-or-dbs]
  (->> (for [db (as-> (or (get-db-name db-or-dbs) "default") dbs
                  (str/split dbs #" ")
                  (remove empty? dbs)
                  (map (comp #(ddl.i/format-name :clickhouse %) str/trim) dbs))]
         (jdbc/with-db-metadata [metadata (->spec db-or-dbs)]
           (jdbc/metadata-result
            (get-tables-from-metadata metadata db))))
       (apply concat)
       (filter not-inner-mv-table?)
       (tables-set)))

(defmethod driver/describe-database :clickhouse
  [_ {{:keys [scan-all-databases]}
      :details :as db}]
  {:tables
   (if
    (boolean scan-all-databases)
     (get-all-tables db)
     (get-tables-in-dbs db))})

(defn- ^:private is-db-required?
  [field]
  (not (str/starts-with? (get-in field [:database-type]) "Nullable")))

(defmethod driver/describe-table :clickhouse
  [_ database table]
  (let [table-metadata (sql-jdbc.sync/describe-table :clickhouse database table)
        filtered-fields (for [field (:fields table-metadata)
                              :let [updated-field (update-in field [:database-required]
                                                             (fn [_] (is-db-required? field)))]
                              ;; Skip all AggregateFunction (but keeping SimpleAggregateFunction) columns
                              ;; JDBC does not support that and it crashes the data browser
                              :when (not (re-matches #"^AggregateFunction\(.+$"
                                                     (get field :database-type)))]
                          updated-field)]
    (merge table-metadata {:fields (set filtered-fields)})))

(defmethod driver/display-name :clickhouse [_] "ClickHouse")

(doseq [[feature supported?] {:standard-deviation-aggregations true
                              :set-timezone                    false
                              :foreign-keys                    (not config/is-test?)
                              :test/jvm-timezone-setting       false
                              :connection-impersonation        true
                              :schemas                         true}]

  (defmethod driver/database-supports? [:clickhouse feature] [_driver _feature _db] supported?))

(defmethod sql-jdbc.sync/db-default-timezone :clickhouse
  [_ spec]
  (let [sql (str "SELECT timezone() AS tz")
        [{:keys [tz]}] (jdbc/query spec sql)]
    tz))

(defmethod driver/db-start-of-week :clickhouse [_] :monday)

(defmethod ddl.i/format-name :clickhouse [_ table-or-field-name]
  (str/replace table-or-field-name #"-" "_"))

;;; ------------------------------------------ User Impersonation ------------------------------------------

(defmethod driver.sql/set-role-statement :clickhouse
  [_ role]
  (format "SET ROLE %s;" role))

(defmethod driver.sql/default-database-role :clickhouse
  [_ _]
  "NONE")
