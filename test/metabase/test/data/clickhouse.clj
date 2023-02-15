(ns metabase.test.data.clickhouse
  "Code for creating / destroying a ClickHouse database from a `DatabaseDefinition`."
  (:require
   [metabase.driver.ddl.interface :as ddl.i]
   [metabase.driver.sql.util :as sql.u]
   [metabase.test.data
    [interface :as tx]
    [sql-jdbc :as sql-jdbc.tx]]
   [metabase.test.data.sql :as sql.tx]
   [metabase.test.data.sql-jdbc
    [execute :as execute]
    [load-data :as load-data]]))

(sql-jdbc.tx/add-test-extensions! :clickhouse)

(def product-name "metabase/1.0.3 clickhouse-jdbc/0.3.2-patch-11")
(def default-connection-params {:classname "com.clickhouse.jdbc.ClickHouseDriver"
                                :subprotocol "clickhouse"
                                :subname "//localhost:8123/default"
                                :user "default"
                                :password ""
                                :ssl false
                                :use_no_proxy false
                                :use_server_time_zone_for_dates true
                                :client_name product-name})

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
