CREATE DATABASE IF NOT EXISTS kafka_integration;

CREATE TABLE IF NOT EXISTS kafka_integration.kafka_numbers_raw
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'numbers',
    kafka_group_name = 'clickhouse_numbers_consumer',
    kafka_format = 'RawBLOB',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS kafka_integration.kafka_numbers_dlq_out
(
    raw String,
    reason String,
    created_at DateTime
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'numbers_dlq',
    kafka_group_name = 'clickhouse_numbers_dlq_producer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS kafka_integration.numbers
(
    value Int64,
    sign Enum8('pos' = 1, 'neg' = -1),
    ingested_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (ingested_at, value);

CREATE TABLE IF NOT EXISTS kafka_integration.sums
(
    key UInt8,
    pos_sum Int64,
    neg_sum Int64
)
ENGINE = SummingMergeTree
ORDER BY key;

CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_integration.mv_numbers_to_dlq
TO kafka_integration.kafka_numbers_dlq_out
AS
SELECT
    raw,
    'parse_error' AS reason,
    now() AS created_at
FROM kafka_integration.kafka_numbers_raw
WHERE toInt64OrNull(trim(BOTH ' \n\r\t' FROM raw)) IS NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_integration.mv_numbers_to_storage
TO kafka_integration.numbers
AS
SELECT
    v AS value,
    if(v >= 0, 'pos', 'neg') AS sign,
    now() AS ingested_at
FROM
(
    SELECT
        toInt64OrNull(trim(BOTH ' \n\r\t' FROM raw)) AS v
    FROM kafka_integration.kafka_numbers_raw
)
WHERE v IS NOT NULL;

CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_integration.mv_numbers_to_sums
TO kafka_integration.sums
AS
SELECT
    toUInt8(1) AS key,
    sumIf(value, value >= 0) AS pos_sum,
    sumIf(value, value < 0) AS neg_sum
FROM kafka_integration.numbers
GROUP BY key;


