CREATE DATABASE IF NOT EXISTS demo;

-- Kafka engine table: streams from Redpanda topic `taps`
CREATE TABLE IF NOT EXISTS demo.taps_kafka (
    ts          DateTime64(3),
    session_id  String,
    device      String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list      = 'redpanda:9092',
    kafka_topic_list       = 'taps',
    kafka_group_name       = 'clickhouse-taps-consumer',
    kafka_format           = 'JSONEachRow',
    kafka_num_consumers    = 1,
    kafka_max_block_size   = 1024,
    kafka_flush_interval_ms = 500;

-- Persistent storage
CREATE TABLE IF NOT EXISTS demo.taps (
    ts          DateTime64(3),
    session_id  String,
    device      String
) ENGINE = MergeTree()
ORDER BY ts;

-- Materialized view: bridge from Kafka stream into the persistent table
CREATE MATERIALIZED VIEW IF NOT EXISTS demo.taps_mv TO demo.taps AS
SELECT ts, session_id, device FROM demo.taps_kafka;
