CREATE TABLE test_input (
    `id` INT primary key,
    name STRING,
    description STRING
) WITH (
    'connector' = 'postgres-cdc-inlong',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'database-name' = 'test',
    'table-name' = 'test_input',
    'schema-name' = 'public',
    'decoding.plugin.name' = 'pgoutput',
    'slot.name' = 'inlong_slot',
    'debezium.slot.name' = 'inlong_slot'
);

CREATE TABLE kafka_load (
    `id` INT NOT NULL,
    name STRING,
    description STRING
) WITH (
    'connector' = 'kafka-inlong',
    'topic' = 'test-topic',
    'scan.startup.mode' = 'earliest-offset',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'csv'
);

CREATE TABLE kafka_extract (
    `id` INT NOT NULL,
    name STRING,
    description STRING
) WITH (
    'connector' = 'kafka-inlong',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'testGroup',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'csv'
);

CREATE TABLE test_output (
    `id` INT primary key,
    name STRING,
    description STRING
) WITH (
    'connector' = 'starrocks-inlong',
    'jdbc-url' = 'jdbc:mysql://starrocks:9030',
    'load-url'='starrocks:8030',
    'database-name'='test',
    'table-name' = 'test_output',
    'username' = 'inlong',
    'password' = 'inlong',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.column_separator' = '\x01',
    'sink.properties.row_delimiter' = '\x02'
);

INSERT INTO kafka_load select * from test_input;
INSERT INTO test_output select * from kafka_extract;




