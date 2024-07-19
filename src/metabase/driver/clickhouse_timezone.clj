(ns metabase.driver.clickhouse-timezone
  (:require    [clojure.core.memoize :as memoize]
               [metabase.driver :as driver]
               [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
               [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
               [metabase.lib.metadata :as lib.metadata]
               [metabase.query-processor.store :as qp.store]))

(set! *warn-on-reflection* true)

;; cache the results for 60 minutes;
;; TTL is here only to eventually clear out old entries/keep it from growing too large
(def ^:private default-cache-ttl (* 60 60 1000))
(def ^:private server-timezone-query "SELECT timezone() AS tz")

(def ^:private ^{:arglists '([db-details])} get-default-timezone*
  (memoize/ttl
   (fn [db-details]
     (sql-jdbc.execute/do-with-connection-with-options
      :clickhouse
      (sql-jdbc.conn/connection-details->spec :clickhouse db-details)
      nil
      (fn [^java.sql.Connection conn]
        (with-open [stmt (.prepareStatement conn server-timezone-query)
                    rset (.executeQuery stmt)]
          (when (.next rset)
            (.getString rset 1))))))
   :ttl/threshold default-cache-ttl))

(defmethod driver/db-default-timezone :clickhouse
  [_driver db]
  (get-default-timezone* (:details db)))

(defn get-default-timezone
  "Used from the QP overrides; we don't have access to the DB object there, so it always uses the current DB"
  []
  (let [current-db (lib.metadata/database (qp.store/metadata-provider))]
    ;; (println "#### current db" current-db)
    (driver/db-default-timezone :clickhouse current-db)))
