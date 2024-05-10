(ns metabase.driver.clickhouse
  "Driver for ClickHouse databases"
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.string :as str]
            [honey.sql :as sql]
            [metabase [config :as config]]
            [metabase.driver :as driver]
            [metabase.driver.clickhouse-introspection]
            [metabase.driver.clickhouse-nippy]
            [metabase.driver.clickhouse-qp]
            [metabase.driver.ddl.interface :as ddl.i]
            [metabase.driver.sql :as driver.sql]
            [metabase.driver.sql-jdbc [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util :as sql.u]
            [metabase.query-processor.writeback :as qp.writeback]
            [metabase.test.data.sql :as sql.tx]
            [metabase.upload :as upload]
            [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

(driver/register! :clickhouse :parent :sql-jdbc)

(defmethod driver/display-name :clickhouse [_] "ClickHouse")
(def ^:private product-name "metabase/1.4.1")

(defmethod driver/prettify-native-form :clickhouse [_ native-form]
  (sql.u/format-sql-and-fix-params :mysql native-form))

(doseq [[feature supported?] {:standard-deviation-aggregations true
                              :foreign-keys                    (not config/is-test?)
                              :set-timezone                    false
                              :convert-timezone                false
                              :test/jvm-timezone-setting       false
                              :connection-impersonation        false
                              :schemas                         true
                              :uploads                         true
                              :datetime-diff                   true
                              :upload-with-auto-pk             false}]

  (defmethod driver/database-supports? [:clickhouse feature] [_driver _feature _db] supported?))

(def ^:private default-connection-details
  {:user "default" :password "" :dbname "default" :host "localhost" :port "8123"})

(defmethod sql-jdbc.conn/connection-details->spec :clickhouse
  [_ details]
  ;; ensure defaults merge on top of nils
  (let [details (reduce-kv (fn [m k v] (assoc m k (or v (k default-connection-details))))
                           default-connection-details
                           details)
        {:keys [user password dbname host port ssl use-no-proxy]} details
        ;; if multiple databases were specified for the connection,
        ;; use only the first dbname as the "main" one
        dbname (first (str/split (str/trim dbname) #" "))]
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

(defmethod driver/can-connect? :clickhouse
  [driver details]
  (if config/is-test?
    (try
      ;; Default SELECT 1 is not enough for Metabase test suite,
      ;; as it works slightly differently than expected there
      (let [spec  (sql-jdbc.conn/connection-details->spec driver details)
            db    (or (:dbname details) (:db details) "default")]
        (sql-jdbc.execute/do-with-connection-with-options
         driver spec nil
         (fn [^java.sql.Connection conn]
           (let [stmt (.prepareStatement conn "SELECT count(*) > 0 FROM system.databases WHERE name = ?")
                 _    (.setString stmt 1 db)
                 rset (.executeQuery stmt)]
             (when (.next rset)
               (.getBoolean rset 1))))))
      (catch Throwable e
        (log/error e "An exception during ClickHouse connectivity check")
        false))
    ;; During normal usage, fall back to the default implementation
    (sql-jdbc.conn/can-connect? driver details)))

(defmethod driver/db-default-timezone :clickhouse
  [driver database]
  (sql-jdbc.execute/do-with-connection-with-options
   driver database nil
   (fn [^java.sql.Connection conn]
     (with-open [stmt (.prepareStatement conn "SELECT timezone() AS tz")
                 rset (.executeQuery stmt)]
       (when (.next rset)
         (.getString rset 1))))))

(defmethod driver/db-start-of-week :clickhouse [_] :monday)

(defmethod ddl.i/format-name :clickhouse [_ table-or-field-name]
  (str/replace table-or-field-name #"-" "_"))

(def ^:private version-query
  "WITH s AS (SELECT version() AS ver, splitByChar('.', ver) AS verSplit) SELECT s.ver, toInt32(verSplit[1]), toInt32(verSplit[2]) FROM s")
(defmethod driver/dbms-version :clickhouse
  [driver database]
  (sql-jdbc.execute/do-with-connection-with-options
    driver database nil
    (fn [^java.sql.Connection conn]
      (with-open [stmt (.prepareStatement conn version-query)
                  rset (.executeQuery stmt)]
        (when (.next rset)
          {:version          (.getString rset 1)
           :semantic-version {:major (.getInt rset 2)
                              :minor (.getInt rset 3)}})))))

(defmethod driver/upload-type->database-type :clickhouse
  [_driver upload-type]
  (case upload-type
    ::upload/varchar-255              "Nullable(String)"
    ::upload/text                     "Nullable(String)"
    ::upload/int                      "Nullable(Int64)"
    ::upload/float                    "Nullable(Float64)"
    ::upload/boolean                  "Nullable(Boolean)"
    ::upload/date                     "Nullable(Date32)"
    ::upload/datetime                 "Nullable(DateTime64(3))"
    ;; FIXME: should be `Nullable(DateTime64(3))`
    ::upload/offset-datetime          nil))

(defmethod driver/table-name-length-limit :clickhouse
  [_driver]
  ;; FIXME: This is a lie because you're really limited by a filesystems' limits, because Clickhouse uses
  ;; filenames as table/column names. But its an approximation
  206)

(defn- quote-name [s]
  (let [parts (str/split (name s) #"\.")]
    (str/join "." (map #(str "`" % "`") parts))))

(defn- create-table!-sql
  [driver table-name column-definitions & {:keys [primary-key]}]
  (str/join "\n"
            [(first (sql/format {:create-table (keyword table-name)
                                 :with-columns (mapv (fn [[name type-spec]]
                                                       (vec (cons name [[:raw type-spec]])))
                                                     column-definitions)}
                                :quoted true
                                :dialect (sql.qp/quote-style driver)))
             "ENGINE = MergeTree"
             (format "PRIMARY KEY (%s)" (str/join ", " (map quote-name primary-key)))
             "ORDER BY ()"]))

(defmethod driver/create-table! :clickhouse
  [driver db-id table-name column-definitions & {:keys [primary-key]}]
  (let [sql (create-table!-sql driver table-name column-definitions :primary-key primary-key)]
    (qp.writeback/execute-write-sql! db-id sql)))

(defmethod driver/insert-into! :clickhouse
  [driver db-id table-name column-names values]
  (when (seq values)
    (sql-jdbc.execute/do-with-connection-with-options
     driver
     db-id
     {:write? true}
     (fn [^java.sql.Connection conn]
       (let [sql (format "insert into %s (%s)" (quote-name table-name) (str/join ", " (map quote-name column-names)))]
         (with-open [ps (.prepareStatement conn sql)]
           (doseq [row values]
             (when (seq row)
               (doseq [[idx v] (map-indexed (fn [x y] [(inc x) y]) row)]
                 (condp isa? (type v)
                   java.lang.String         (.setString ps idx v)
                   java.lang.Boolean        (.setBoolean ps idx v)
                   java.lang.Long           (.setLong ps idx v)
                   java.lang.Double         (.setFloat ps idx v)
                   java.math.BigInteger     (.setObject ps idx v)
                   java.time.LocalDate      (.setObject ps idx v)
                   java.time.LocalDateTime  (.setObject ps idx v)
                   (.setString ps idx v)))
               (.addBatch ps)))
           (doall (.executeBatch ps))))))))

;;; ------------------------------------------ User Impersonation ------------------------------------------

(defmethod driver.sql/set-role-statement :clickhouse
  [_ role]
  (format "SET ROLE %s;" role))

(defmethod driver.sql/default-database-role :clickhouse
  [_ _]
  "NONE")
