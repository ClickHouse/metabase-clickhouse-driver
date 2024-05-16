(ns metabase.driver.clickhouse
  "Driver for ClickHouse databases"
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require [clojure.string :as str]
            [metabase [config :as config]]
            [metabase.driver :as driver]
            [metabase.driver.clickhouse-introspection]
            [metabase.driver.clickhouse-nippy]
            [metabase.driver.clickhouse-qp]
            [metabase.driver.clickhouse-version :as clickhouse-version]
            [metabase.driver.ddl.interface :as ddl.i]
            [metabase.driver.sql :as driver.sql]
            [metabase.driver.sql-jdbc [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql.util :as sql.u]
            [metabase.models]
            [metabase.util.log :as log]))

(set! *warn-on-reflection* true)

(driver/register! :clickhouse :parent :sql-jdbc)

(defmethod driver/display-name :clickhouse [_] "ClickHouse")
(def ^:private product-name "metabase/1.5.0")

(defmethod driver/prettify-native-form :clickhouse
  [_ native-form]
  (sql.u/format-sql-and-fix-params :mysql native-form))

(doseq [[feature supported?] {:standard-deviation-aggregations true
                              :foreign-keys                    (not config/is-test?)
                              :set-timezone                    false
                              :convert-timezone                false
                              :test/jvm-timezone-setting       false
                              :schemas                         true
                              :datetime-diff                   true}]
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
      :product_name product-name
      ;; addresses breaking changes from the 0.5.0 JDBC driver release
      ;; see https://github.com/ClickHouse/clickhouse-java/releases/tag/v0.5.0
      ;; and https://github.com/ClickHouse/clickhouse-java/issues/1634#issuecomment-2110392634
      :databaseTerm "schema"}
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

(defmethod ddl.i/format-name :clickhouse
  [_ table-or-field-name]
  (str/replace table-or-field-name #"-" "_"))

;;; ------------------------------------------ Connection Impersonation ------------------------------------------

(defmethod driver/database-supports? [:clickhouse :connection-impersonation]
  [_driver _feature db]
  (clickhouse-version/is-at-least? 24 4 db))

(defmethod driver.sql/set-role-statement :clickhouse
  [_ role]
  (format "SET ROLE %s;" role))

(defmethod driver.sql/default-database-role :clickhouse
  [_ _]
  "NONE")
