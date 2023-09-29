(ns metabase.test.data.clickhouse
  "Code for creating / destroying a ClickHouse database from a `DatabaseDefinition`."
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [metabase.driver.ddl.interface :as ddl.i]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql.util :as sql.u]
   [metabase.models [database :refer [Database]]]
   [metabase.query-processor-test :as qp.test]
   [metabase.sync.sync-metadata :as sync-metadata]
   [metabase.test.data
    [interface :as tx]
    [sql-jdbc :as sql-jdbc.tx]]
   [metabase.test.data.sql :as sql.tx]
   [metabase.test.data.sql-jdbc
    [execute :as execute]
    [load-data :as load-data]]
   [toucan2.tools.with-temp :as t2.with-temp]))

(sql-jdbc.tx/add-test-extensions! :clickhouse)

(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Boolean]    [_ _] "Boolean")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/BigInteger] [_ _] "Int64")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Char]       [_ _] "String")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Date]       [_ _] "Date")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/DateTime]   [_ _] "DateTime64")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Float]      [_ _] "Float64")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Integer]    [_ _] "Nullable(Int32)")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/IPAddress]  [_ _] "IPv4")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Text]       [_ _] "Nullable(String)")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/UUID]       [_ _] "UUID")
(defmethod sql.tx/field-base-type->sql-type [:clickhouse :type/Time]       [_ _] "DateTime64")

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

(defmethod sql.tx/create-table-sql :clickhouse
  [driver {:keys [database-name]} {:keys [table-name field-definitions]}]
  (let [quot          #(sql.u/quote-name driver :field (ddl.i/format-name driver %))
        pk-field-name (quot (sql.tx/pk-field-name driver))]
    (format "CREATE TABLE %s (%s %s, %s) ENGINE = Memory"
            (sql.tx/qualify-and-quote driver database-name table-name)
            pk-field-name
            (sql.tx/pk-sql-type driver)
            (->> field-definitions
                 (map (fn [{:keys [field-name base-type]}]
                        (format "%s %s" (quot field-name)
                                (if (map? base-type)
                                  (:native base-type)
                                  (sql.tx/field-base-type->sql-type driver base-type)))))
                 (interpose ", ")
                 (apply str)))))

(defmethod execute/execute-sql! :clickhouse [& args]
  (apply execute/sequentially-execute-sql! args))

(defmethod load-data/load-data! :clickhouse [& args]
  (apply load-data/load-data-add-ids! args))

(defmethod sql.tx/pk-sql-type :clickhouse [_] "Nullable(Int32)")

(defmethod sql.tx/add-fk-sql :clickhouse [& _] nil) ; TODO - fix me

(defmethod tx/supports-time-type? :clickhouse [_driver] false)

(def default-connection-params
  {:classname "com.clickhouse.jdbc.ClickHouseDriver"
   :subprotocol "clickhouse"
   :subname "//localhost:8123/default"
   :user "default"
   :password ""
   :ssl false
   :use_no_proxy false
   :use_server_time_zone_for_dates true
   :product_name "metabase/1.2.2"})

(defn rows-without-index
  "Remove the Metabase index which is the first column in the result set"
  [query-result]
  (map #(drop 1 %) (qp.test/rows query-result)))

(defn- test-db-details
  []
  {:engine :clickhouse
   :details (tx/dbdef->connection-details
             :clickhouse :db {:database-name "metabase_test"})})

(def test-db-initialized? (atom false))
(defn- create-test-db!
  "Create a ClickHouse database called `metabase_test` and initialize some test data"
  []
  (jdbc/with-db-connection
    [conn (sql-jdbc.conn/connection-details->spec :clickhouse (test-db-details))]
    (let [statements (as-> (slurp "modules/drivers/clickhouse/test/metabase/test/data/datasets.sql") s
                       (str/split s #";")
                       (map str/trim s)
                       (filter seq s))]
      (jdbc/db-do-commands conn statements)
      (reset! test-db-initialized? true))))

(defn do-with-test-db
  "Execute a test function using the test dataset"
  {:style/indent 0}
  [f]
  (when (not @test-db-initialized?) (create-test-db!))
  (t2.with-temp/with-temp
    [Database database (test-db-details)]
    (sync-metadata/sync-db-metadata! database)
    (f database)))
