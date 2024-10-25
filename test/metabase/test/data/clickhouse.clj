(ns metabase.test.data.clickhouse
  "Code for creating / destroying a ClickHouse database from a `DatabaseDefinition`."
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [metabase.driver.ddl.interface :as ddl.i]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql.util :as sql.u]
   [metabase.models.database :refer [Database]]
   [metabase.query-processor.test-util :as qp.test]
   [metabase.sync.sync-metadata :as sync-metadata]
   [metabase.test.data.interface :as tx]
   [metabase.test.data.sql :as sql.tx]
   [metabase.test.data.sql-jdbc :as sql-jdbc.tx]
   [metabase.test.data.sql-jdbc.execute :as execute]
   [metabase.test.data.sql-jdbc.load-data :as load-data]
   [metabase.util.log :as log]
   [toucan2.tools.with-temp :as t2.with-temp])
  (:import    [com.clickhouse.jdbc.internal ClickHouseStatementImpl]))

(sql-jdbc.tx/add-test-extensions! :clickhouse)

(def default-connection-params
  {:classname "com.clickhouse.jdbc.ClickHouseDriver"
   :subprotocol "clickhouse"
   :subname "//localhost:8123/default"
   :user "default"
   :password ""
   :ssl false
   :use_no_proxy false
   :use_server_time_zone_for_dates true
   :product_name "metabase/1.51.0"
   :databaseTerm "schema"
   :remember_last_set_roles true
   :http_connection_provider "HTTP_URL_CONNECTION"
   :custom_http_params ""})

(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Boolean]         [_ _] "Boolean")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/BigInteger]      [_ _] "Int64")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Char]            [_ _] "String")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Date]            [_ _] "Date")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/DateTime]        [_ _] "DateTime64")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/DateTimeWithTZ]  [_ _] "DateTime64(3, 'UTC')")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Float]           [_ _] "Float64")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Integer]         [_ _] "Int32")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/IPAddress]       [_ _] "IPv4")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Text]            [_ _] "String")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/UUID]            [_ _] "UUID")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Time]            [_ _] "Time")

(defmethod tx/sorts-nil-first? :clickhouse [_ _] false)

(defmethod tx/dbdef->connection-details :clickhouse [_ context {:keys [database-name]}]
  (merge
   {:host     (tx/db-test-env-var-or-throw :clickhouse :host "localhost")
    :port     (tx/db-test-env-var-or-throw :clickhouse :port 8123)
    :timezone :America/Los_Angeles}
   (when-let [user (tx/db-test-env-var :clickhouse :user)]
     {:user user})
   (when-let [password (tx/db-test-env-var :clickhouse :password)]
     {:password password})
   (when (= context :db)
     {:db database-name})))

(defmethod sql.tx/qualified-name-components :clickhouse
  ([_ db-name]                       [db-name])
  ([_ db-name table-name]            [db-name table-name])
  ([_ db-name table-name field-name] [db-name table-name field-name]))

(defmethod tx/create-db! :clickhouse
  [driver {:keys [database-name], :as db-def} & options]
  (let [database-name (ddl.i/format-name driver database-name)]
    (log/infof "Creating ClickHouse database %s" (pr-str database-name))
    ;; call the default impl for SQL JDBC drivers
    (apply (get-method tx/create-db! :sql-jdbc/test-extensions) driver db-def options)))

(defn- quote-name
  [name]
  (sql.u/quote-name :clickhouse :field (ddl.i/format-name :clickhouse name)))

(def ^:private non-nullable-types ["Array" "Map" "Tuple"])
(defn- disallowed-as-nullable?
  [ch-type]
  (boolean (some #(str/starts-with? ch-type %) non-nullable-types)))

(defn- field->clickhouse-column
  [field]
  (let [{:keys [field-name base-type pk?]} field
        ch-type  (if (map? base-type)
                   (:native base-type)
                   (sql.tx/field-base-type->sql-type :clickhouse base-type))
        col-name (quote-name field-name)
        ch-col   (cond
                   (or pk? (disallowed-as-nullable? ch-type))
                   (format "%s %s" col-name ch-type)
                   (= ch-type "Time")
                   (format "%s Nullable(DateTime64) COMMENT 'time'" col-name)
                   ; _
                   :else (format "%s Nullable(%s)" col-name ch-type))]
    ch-col))

(defn- ->comma-separated-str
  [coll]
  (->> coll
       (interpose ", ")
       (apply str)))

(defmethod sql.tx/create-table-sql :clickhouse
  [_ {:keys [database-name]} {:keys [table-name field-definitions]}]
  (let [table-name     (sql.tx/qualify-and-quote :clickhouse database-name table-name)
        pk-fields      (filter (fn [{:keys [pk?]}] pk?) field-definitions)
        pk-field-names (map #(quote-name (:field-name %)) pk-fields)
        fields         (->> field-definitions
                            (map field->clickhouse-column)
                            (->comma-separated-str))
        order-by       (->comma-separated-str pk-field-names)]
    (format "CREATE TABLE %s (%s)
             ENGINE = MergeTree
             ORDER BY (%s)
             SETTINGS allow_nullable_key=1"
            table-name fields order-by)))

(defmethod execute/execute-sql! :clickhouse [& args]
  (apply execute/sequentially-execute-sql! args))

(defmethod load-data/row-xform :clickhouse [_driver _dbdef tabledef]
  (load-data/maybe-add-ids-xform tabledef))

(defmethod sql.tx/pk-sql-type :clickhouse [_] "Int32")

(defmethod sql.tx/add-fk-sql :clickhouse [& _] nil)

(defmethod sql.tx/session-schema :clickhouse [_] "default")

(defn rows-without-index
  "Remove the Metabase index which is the first column in the result set"
  [query-result]
  (map #(drop 1 %) (qp.test/rows query-result)))

(def ^:private test-db-initialized? (atom false))
(defn create-test-db!
  "Create a ClickHouse database called `metabase_test` and initialize some test data"
  [f]
  (when (not @test-db-initialized?)
    (let [details (tx/dbdef->connection-details :clickhouse :db {:database-name "metabase_test"})]
      ;; (println "Executing create-test-db! with details:" details)
      (jdbc/with-db-connection
        [spec (sql-jdbc.conn/connection-details->spec :clickhouse (merge {:engine :clickhouse} details))]
        (let [statements (as-> (slurp "modules/drivers/clickhouse/test/metabase/test/data/datasets.sql") s
                           (str/split s #";")
                           (map str/trim s)
                           (filter seq s))]
          (jdbc/db-do-commands spec statements)
          (reset! test-db-initialized? true)))))
  (f))

#_{:clj-kondo/ignore [:warn-on-reflection]}
(defn exec-statements
  ([statements details-map]
   (exec-statements statements details-map nil))
  ([statements details-map clickhouse-settings]
   (sql-jdbc.execute/do-with-connection-with-options
    :clickhouse
    (sql-jdbc.conn/connection-details->spec :clickhouse (merge {:engine :clickhouse} details-map))
    {:write? true}
    (fn [^java.sql.Connection conn]
      (doseq [statement statements]
        ;; (println "Executing:" statement)
        (with-open [jdbcStmt (.createStatement conn)]
          (let [^ClickHouseStatementImpl clickhouseStmt (.unwrap jdbcStmt ClickHouseStatementImpl)
                request (.getRequest clickhouseStmt)]
            (when clickhouse-settings
              (doseq [[k v] clickhouse-settings] (.set request k v)))
            (with-open [_response (-> request
                                      (.query ^String statement)
                                      (.executeAndWait))]))))))))

(defn do-with-test-db
  "Execute a test function using the test dataset"
  [f]
  (t2.with-temp/with-temp
    [Database database
     {:engine :clickhouse
      :details (tx/dbdef->connection-details :clickhouse :db {:database-name "metabase_test"})}]
    (sync-metadata/sync-db-metadata! database)
    (f database)))

(defmethod tx/dataset-already-loaded? :clickhouse
  [driver dbdef]
  (let [tabledef       (first (:table-definitions dbdef))
        db-name        (ddl.i/format-name :clickhouse (:database-name dbdef))
        table-name     (ddl.i/format-name :clickhouse (:table-name tabledef))
        details        (tx/dbdef->connection-details :clickhouse :db {:database-name db-name})]
    (sql-jdbc.execute/do-with-connection-with-options
     driver
     (sql-jdbc.conn/connection-details->spec driver details)
     {:write? false}
     (fn [^java.sql.Connection conn]
       (with-open [rset (.getTables (.getMetaData conn)
                                    #_catalog        nil
                                    #_schema-pattern db-name
                                    #_table-pattern  table-name
                                    #_types          (into-array String ["TABLE"]))]
         ;; if the ResultSet returns anything we know the table is already loaded.
         (.next rset))))))
