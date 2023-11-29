(ns metabase.driver.clickhouse-test
  #_{:clj-kondo/ignore [:unsorted-required-namespaces]}
  (:require
   [clojure.test :refer :all]
   [metabase.driver :as driver]
   [metabase.test :as mt]
   [metabase.test.data.clickhouse :as ctd]))

(defn- desc-table
  [table-name]
  (into #{} (map #(select-keys % [:name :database-type :base-type :database-required])
                 (:fields (ctd/do-with-test-db
                           #(driver/describe-table :clickhouse % {:name table-name}))))))

(deftest clickhouse-base-types-test
  (mt/test-driver
   :clickhouse
   (testing "enums"
     (let [table-name "enums_base_types"]
       (is (= #{{:base-type :type/Text,
                 :database-required false,
                 :database-type "Nullable(Enum8('America/New_York' = 1))",
                 :name "c1"}
                {:base-type :type/Text,
                 :database-required true,
                 :database-type "Enum8('BASE TABLE' = 1, 'VIEW' = 2, 'FOREIGN TABLE' = 3, 'LOCAL TEMPORARY' = 4, 'SYSTEM VIEW' = 5)",
                 :name "c2"}
                {:base-type :type/Text,
                 :database-required true,
                 :database-type "Enum8('NO' = 1, 'YES' = 2)",
                 :name "c3"}
                {:base-type :type/Text,
                 :database-required true,
                 :database-type "Enum16('SHOW DATABASES' = 0, 'SHOW TABLES' = 1, 'SHOW COLUMNS' = 2)",
                 :name "c4"}
                {:base-type :type/Text,
                 :database-required false,
                 :database-type "Nullable(Enum8('GLOBAL' = 0, 'DATABASE' = 1, 'TABLE' = 2))",
                 :name "c5"}
                {:base-type :type/Text,
                 :database-required false,
                 :database-type "Nullable(Enum16('SHOW DATABASES' = 0, 'SHOW TABLES' = 1, 'SHOW COLUMNS' = 2))",
                 :name "c6"}}
              (desc-table table-name)))))
   (testing "dates"
     (let [table-name "date_base_types"]
       (is (= #{{:base-type :type/Date,
                 :database-required true,
                 :database-type "Date",
                 :name "c1"}
                {:base-type :type/Date,
                 :database-required true,
                 :database-type "Date32",
                 :name "c2"}
                {:base-type :type/Date,
                 :database-required false,
                 :database-type "Nullable(Date)",
                 :name "c3"}
                {:base-type :type/Date,
                 :database-required false,
                 :database-type "Nullable(Date32)",
                 :name "c4"}}
              (desc-table table-name)))))
   (testing "datetimes"
     (let [table-name "datetime_base_types"]
       (is (= #{{:base-type :type/DateTime,
                 :database-required false,
                 :database-type "Nullable(DateTime('America/New_York'))",
                 :name "c1"}
                {:base-type :type/DateTime,
                 :database-required true,
                 :database-type "DateTime('America/New_York')",
                 :name "c2"}
                {:base-type :type/DateTime,
                 :database-required true,
                 :database-type "DateTime",
                 :name "c3"}
                {:base-type :type/DateTime,
                 :database-required true,
                 :database-type "DateTime64(3)",
                 :name "c4"}
                {:base-type :type/DateTime,
                 :database-required true,
                 :database-type "DateTime64(9, 'America/New_York')",
                 :name "c5"}
                {:base-type :type/DateTime,
                 :database-required false,
                 :database-type "Nullable(DateTime64(6, 'America/New_York'))",
                 :name "c6"}
                {:base-type :type/DateTime,
                 :database-required false,
                 :database-type "Nullable(DateTime64(0))",
                 :name "c7"}
                {:base-type :type/DateTime,
                 :database-required false,
                 :database-type "Nullable(DateTime)",
                 :name "c8"}}
              (desc-table table-name)))))
   (testing "integers"
     (let [table-name "integer_base_types"]
       (is (= #{{:base-type :type/Integer,
                 :database-required true,
                 :database-type "UInt8",
                 :name "c1"}
                {:base-type :type/Integer,
                 :database-required true,
                 :database-type "UInt16",
                 :name "c2"}
                {:base-type :type/Integer,
                 :database-required true,
                 :database-type "UInt32",
                 :name "c3"}
                {:base-type :type/BigInteger,
                 :database-required true,
                 :database-type "UInt64",
                 :name "c4"}
                {:base-type :type/*,
                 :database-required true,
                 :database-type "UInt128",
                 :name "c5"}
                {:base-type :type/*,
                 :database-required true,
                 :database-type "UInt256",
                 :name "c6"}
                {:base-type :type/Integer,
                 :database-required true,
                 :database-type "Int8",
                 :name "c7"}
                {:base-type :type/Integer,
                 :database-required true,
                 :database-type "Int16",
                 :name "c8"}
                {:base-type :type/Integer,
                 :database-required true,
                 :database-type "Int32",
                 :name "c9"}
                {:base-type :type/BigInteger,
                 :database-required true,
                 :database-type "Int64",
                 :name "c10"}
                {:base-type :type/*,
                 :database-required true,
                 :database-type "Int128",
                 :name "c11"}
                {:base-type :type/*,
                 :database-required true,
                 :database-type "Int256",
                 :name "c12"}
                {:base-type :type/Integer,
                 :database-required false,
                 :database-type "Nullable(Int32)",
                 :name "c13"}}
              (desc-table table-name)))))
   (testing "numerics"
     (let [table-name "numeric_base_types"]
       (is (= #{{:base-type :type/Float,
                 :database-required true,
                 :database-type "Float32",
                 :name "c1"}
                {:base-type :type/Float,
                 :database-required true,
                 :database-type "Float64",
                 :name "c2"}
                {:base-type :type/Decimal,
                 :database-required true,
                 :database-type "Decimal(4, 2)",
                 :name "c3"}
                {:base-type :type/Decimal,
                 :database-required true,
                 :database-type "Decimal(9, 7)",
                 :name "c4"}
                {:base-type :type/Decimal,
                 :database-required true,
                 :database-type "Decimal(18, 12)",
                 :name "c5"}
                {:base-type :type/Decimal,
                 :database-required true,
                 :database-type "Decimal(38, 24)",
                 :name "c6"}
                {:base-type :type/Decimal,
                 :database-required true,
                 :database-type "Decimal(76, 42)",
                 :name "c7"}
                {:base-type :type/Float,
                 :database-required false,
                 :database-type "Nullable(Float32)",
                 :name "c8"}
                {:base-type :type/Decimal,
                 :database-required false,
                 :database-type "Nullable(Decimal(4, 2))",
                 :name "c9"}
                {:base-type :type/Decimal,
                 :database-required false,
                 :database-type "Nullable(Decimal(76, 42))",
                 :name "c10"}}
              (desc-table table-name)))))
   (testing "strings"
     (let [table-name "string_base_types"]
       (is (= #{{:base-type :type/Text,
                 :database-required true,
                 :database-type "String",
                 :name "c1"}
                {:base-type :type/Text,
                 :database-required true,
                 :database-type "LowCardinality(String)",
                 :name "c2"}
                {:base-type :type/TextLike,
                 :database-required true,
                 :database-type "FixedString(32)",
                 :name "c3"}
                {:base-type :type/Text,
                 :database-required false,
                 :database-type "Nullable(String)",
                 :name "c4"}
                {:base-type :type/TextLike,
                 :database-required true,
                 :database-type "LowCardinality(FixedString(4))",
                 :name "c5"}}
              (desc-table table-name)))))
   (testing "arrays"
     (let [table-name "array_base_types"]
       (is (= #{{:base-type :type/Array,
                 :database-required true,
                 :database-type "Array(String)",
                 :name "c1"}
                {:base-type :type/Array,
                 :database-required true,
                 :database-type "Array(Nullable(Int32))",
                 :name "c2"}
                {:base-type :type/Array,
                 :database-required true,
                 :database-type "Array(Array(LowCardinality(FixedString(32))))",
                 :name "c3"}
                {:base-type :type/Array,
                 :database-required true,
                 :database-type "Array(Array(Array(String)))",
                 :name "c4"}}
              (desc-table table-name)))))
   (testing "low cardinality nullable"
     (let [table-name "low_cardinality_nullable_base_types"]
       (is (= #{{:base-type :type/Text,
                 :database-required true,
                 :database-type "LowCardinality(Nullable(String))",
                 :name "c1"}
                {:base-type :type/TextLike,
                 :database-required true,
                 :database-type "LowCardinality(Nullable(FixedString(16)))",
                 :name "c2"}}
              (desc-table table-name)))))
   (testing "everything else"
     (let [table-name "misc_base_types"]
       (is (= #{{:base-type :type/Boolean,
                 :database-required true,
                 :database-type "Bool",
                 :name "c1"}
                {:base-type :type/UUID,
                 :database-required true,
                 :database-type "UUID",
                 :name "c2"}
                {:base-type :type/IPAddress,
                 :database-required true,
                 :database-type "IPv4",
                 :name "c3"}
                {:base-type :type/IPAddress,
                 :database-required true,
                 :database-type "IPv6",
                 :name "c4"}
                {:base-type :type/Dictionary,
                 :database-required true,
                 :database-type "Map(Int32, String)",
                 :name "c5"}
                {:base-type :type/Boolean,
                 :database-required false,
                 :database-type "Nullable(Bool)",
                 :name "c6"}
                {:base-type :type/UUID,
                 :database-required false,
                 :database-type "Nullable(UUID)",
                 :name "c7"}
                {:base-type :type/IPAddress,
                 :database-required false,
                 :database-type "Nullable(IPv4)",
                 :name "c8"}
                {:base-type :type/IPAddress,
                 :database-required false,
                 :database-type "Nullable(IPv6)",
                 :name "c9"}
                {:base-type :type/*,
                 :database-required true,
                 :database-type "Tuple(String, Int32)",
                 :name "c10"}}
              (desc-table table-name)))))))
