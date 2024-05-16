(ns metabase.driver.clickhouse
  "Driver for ClickHouse databases"
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.core.memoize :as memoize]
            [clojure.string :as str]
            [honey.sql :as sql]
            [metabase [config :as config]]
            [metabase.db.connection :as mdb.connection]
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
            [metabase.upload :as upload]
            [metabase.util.log :as log])
  (:import [com.clickhouse.client ClickHouseRequest]
           [com.clickhouse.jdbc.internal ClickHouseStatementImpl]))

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
                              :datetime-diff                   true
                              :upload-with-auto-pk             false}]

  (defmethod driver/database-supports? [:clickhouse feature] [_driver _feature _db] supported?))

(def ^:private ^{:arglists '([db])} cloud?
  "Is this a cloud DB?"
  (memoize/ttl
   ^{::memoize/args-fn (fn [[db]]
                         [(mdb.connection/unique-identifier) (:details db)])}
   (fn [db]
     (sql-jdbc.execute/do-with-connection-with-options
      :clickhouse db nil
      (fn [^java.sql.Connection conn]
        (with-open [stmt (.prepareStatement conn "SELECT value='1' FROM system.settings WHERE name='cloud_mode'")
                    rset (.executeQuery stmt)]
          (when (.next rset)
            (.getBoolean rset 1))))))
   ;; cache the results for 60 minutes; TTL is here only to eventually clear out old entries/keep it from growing too
   ;; large
   :ttl/threshold (* 60 60 1000)))

(defmethod driver/database-supports? [:clickhouse :uploads] [_driver _feature db]
  (cloud? db))

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
      ;; select_sequential_consistency is needed to guarantee that we can query data from any replica in CH Cloud
      ;; immediately after it is written
      :select_sequential_consistency true
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
  "Creates a ClickHouse table with the given name and column definitions. It assumes the engine is MergeTree,
   so it only works with Clickhouse Cloud and single node on-premise deployments at the moment."
  [driver table-name column-definitions & {:keys [primary-key]}]
  (str/join "\n"
            [(first (sql/format {:create-table (keyword table-name)
                                 :with-columns (mapv (fn [[name type-spec]]
                                                       (vec (cons name [[:raw type-spec]])))
                                                     column-definitions)}
                                :quoted true
                                :dialect (sql.qp/quote-style driver)))
             "ENGINE = MergeTree"
             (format "ORDER BY (%s)" (str/join ", " (map quote-name primary-key)))]))

(defmethod driver/create-table! :clickhouse
  [driver db-id table-name column-definitions & {:keys [primary-key]}]
  (sql-jdbc.execute/do-with-connection-with-options
   driver
   db-id
   {:write? true}
   (fn [^java.sql.Connection conn]
     (with-open [stmt (.createStatement conn)]
       (let [^ClickHouseStatementImpl stmt (.unwrap stmt ClickHouseStatementImpl)
             request (.getRequest stmt)]
         (.set request "wait_end_of_query" "1")
         (with-open [_response (-> request
                                   (.query ^String (create-table!-sql driver table-name column-definitions :primary-key primary-key))
                                   (.executeAndWait))]))))))

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
