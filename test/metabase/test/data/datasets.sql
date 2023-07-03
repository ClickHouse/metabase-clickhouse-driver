DROP DATABASE IF EXISTS `metabase_test`;
CREATE DATABASE `metabase_test`;

CREATE TABLE `metabase_test`.`enums_test`
(
    enum1 Enum8('foo' = 0, 'bar' = 1, 'foo bar' = 2),
    enum2 Enum16('click' = 0, 'house' = 1),
    enum3 Enum8('qaz' = 42, 'qux' = 23)
) ENGINE = Memory;

INSERT INTO `metabase_test`.`enums_test` (enum1, enum2, enum3)
VALUES ('foo', 'house', 'qaz'),
       ('foo bar', 'click', 'qux'),
       ('bar', 'house', 'qaz');

CREATE TABLE `metabase_test`.`ipaddress_test`
(
    ipvfour Nullable(IPv4),
    ipvsix  Nullable(IPv6)
) Engine = Memory;

INSERT INTO `metabase_test`.`ipaddress_test` (ipvfour, ipvsix)
VALUES (toIPv4('127.0.0.1'), toIPv6('127.0.0.1')),
       (toIPv4('0.0.0.0'),   toIPv6('0.0.0.0')),
       (null, null);

CREATE TABLE `metabase_test`.`boolean_test`
(
    ID Int32,
    b1 Bool,
    b2 Nullable(Bool)
) ENGINE = Memory;

INSERT INTO `metabase_test`.`boolean_test` (ID, b1, b2)
VALUES (1, true, true),
       (2, false, true),
       (3, true, false);

CREATE TABLE `metabase_test`.`maps_test`
(
    m Map(String, UInt64)
) ENGINE = Memory;

INSERT INTO `metabase_test`.`maps_test`
VALUES ({'key1':1,'key2':10}),
       ({'key1':2,'key2':20}),
       ({'key1':3,'key2':30});

-- Used for testing that AggregateFunction columns are excluded,
-- while SimpleAggregateFunction columns are preserved
CREATE TABLE `metabase_test`.`aggregate_functions_filter_test`
(
    idx UInt8,
    a AggregateFunction(uniq, String),
    lowest_value SimpleAggregateFunction(min, UInt8),
    count SimpleAggregateFunction(sum, Int64)
) ENGINE Memory;

INSERT INTO `metabase_test`.`aggregate_functions_filter_test`
    (idx, lowest_value, count)
VALUES (42, 144, 255255);

-- Materialized views (testing .inner tables exclusion)
CREATE TABLE `metabase_test`.`wikistat`
(
    `date`    Date,
    `project` LowCardinality(String),
    `hits`    UInt32
) ENGINE = Memory;

CREATE MATERIALIZED VIEW `metabase_test`.`wikistat_mv` ENGINE =Memory AS
SELECT date, project, sum(hits) AS hits
FROM `metabase_test`.`wikistat`
GROUP BY date, project;

INSERT INTO `metabase_test`.`wikistat`
VALUES (now(), 'foo', 10),
       (now(), 'bar', 10),
       (now(), 'bar', 20);

-- Used in sum-where tests
CREATE TABLE `metabase_test`.`sum_if_test_int`
(
    id            Int64,
    int_value     Int64,
    discriminator String
) ENGINE = Memory;

INSERT INTO `metabase_test`.`sum_if_test_int`
VALUES (1, 1, 'foo'),
       (2, 1, 'foo'),
       (3, 3, 'bar'),
       (4, 5, 'bar');

CREATE TABLE `metabase_test`.`sum_if_test_float`
(
    id            Int64,
    float_value   Float64,
    discriminator String
) ENGINE = Memory;

INSERT INTO `metabase_test`.`sum_if_test_float`
VALUES (1, 1.1,  'foo'),
       (2, 1.44, 'foo'),
       (3, 3.5,  'bar'),
       (4, 5.77, 'bar');

-- Temporal bucketing tests
CREATE TABLE `metabase_test`.`temporal_bucketing`
(
    start_of_year DateTime,
    mid_of_year   DateTime,
    end_of_year   DateTime
) ENGINE = Memory;

INSERT INTO `metabase_test`.`temporal_bucketing`
VALUES ('2022-01-01 00:00:00', '2022-06-20 06:32:54', '2022-12-31 23:59:59');

DROP DATABASE IF EXISTS `metabase_db_scan_test`;
CREATE DATABASE `metabase_db_scan_test`;

CREATE TABLE `metabase_db_scan_test`.`table1` (i Int32) ENGINE = Memory;
CREATE TABLE `metabase_db_scan_test`.`table2` (i Int64) ENGINE = Memory;

-- use-has-token-for-contains setting (LIKE %% vs hasToken) test
CREATE TABLE `metabase_test`.`use_has_token_for_contains_test`
(str String) ENGINE = Memory;

INSERT INTO `metabase_test`.`use_has_token_for_contains_test`
VALUES ('Fred'), ('FRED'), ('red'), ('Red');
