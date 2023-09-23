/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.sort.tests;

import org.apache.inlong.sort.tests.utils.FlinkContainerTestEnv;
import org.apache.inlong.sort.tests.utils.JdbcProxy;
import org.apache.inlong.sort.tests.utils.PlaceholderResolver;
import org.apache.inlong.sort.tests.utils.StarRocksContainer;
import org.apache.inlong.sort.tests.utils.TestUtils;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * End-to-end tests for sort-connector-kafka uber jar.
 */
public class KafkaE2EITCase extends FlinkContainerTestEnv {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaE2EITCase.class);
    private static final Logger STAR_ROCKS_LOG = LoggerFactory.getLogger(StarRocksContainer.class);

    private static final Path kafkaJar = TestUtils.getResource("sort-connector-kafka.jar");
    private static final Path postgresJar = TestUtils.getResource("sort-connector-postgres-cdc.jar");
    private static final Path starrocksJar = TestUtils.getResource("sort-connector-starrocks.jar");
    private static final Path mysqlJdbcJar = TestUtils.getResource("mysql-driver.jar");
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    // ----------------------------------------------------------------------------------------
    // StarRocks Variables
    // ----------------------------------------------------------------------------------------
    private static final String INTER_CONTAINER_STAR_ROCKS_ALIAS = "starrocks";
    private static final String NEW_STARROCKS_REPOSITORY = "inlong-starrocks";
    private static final String NEW_STARROCKS_TAG = "latest";
    private static final String STAR_ROCKS_IMAGE_NAME = "starrocks/allin1-ubi:3.0.4";

    static {
        buildStarRocksImage();
    }

    private static void buildStarRocksImage() {
        GenericContainer oldStarRocks = new GenericContainer(STAR_ROCKS_IMAGE_NAME);
        Startables.deepStart(Stream.of(oldStarRocks)).join();
        oldStarRocks.copyFileToContainer(MountableFile.forClasspathResource("/docker/starrocks/start_fe_be.sh"),
                "/data/deploy/");
        try {
            oldStarRocks.execInContainer("chmod", "+x", "/data/deploy/start_fe_be.sh");
        } catch (Exception e) {
            e.printStackTrace();
        }
        oldStarRocks.getDockerClient()
                .commitCmd(oldStarRocks.getContainerId())
                .withRepository(NEW_STARROCKS_REPOSITORY)
                .withTag(NEW_STARROCKS_TAG).exec();
        oldStarRocks.stop();
    }

    private static String getNewStarRocksImageName() {
        return NEW_STARROCKS_REPOSITORY + ":" + NEW_STARROCKS_TAG;
    }

    @ClassRule
    public static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                    .withNetwork(NETWORK)
                    .withNetworkAliases("kafka")
                    .withEmbeddedZookeeper()
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static final PostgreSQLContainer POSTGRES = (PostgreSQLContainer) new PostgreSQLContainer(
            DockerImageName.parse("debezium/postgres:13").asCompatibleSubstituteFor("postgres"))
                    .withUsername("flinkuser")
                    .withPassword("flinkpw")
                    .withDatabaseName("test")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("postgres")
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @ClassRule
    public static StarRocksContainer STAR_ROCKS =
            (StarRocksContainer) new StarRocksContainer(getNewStarRocksImageName())
                    .withExposedPorts(9030, 8030, 8040)
                    .withNetwork(NETWORK)
                    .withAccessToHost(true)
                    .withNetworkAliases(INTER_CONTAINER_STAR_ROCKS_ALIAS)
                    .withLogConsumer(new Slf4jLogConsumer(STAR_ROCKS_LOG));

    @Before
    public void setup() {
        waitUntilJobRunning(Duration.ofSeconds(30));
        initializePostgresTable();
        initializeStarRocksTable();
    }

    @AfterClass
    public static void teardown() {
        if (KAFKA != null) {
            KAFKA.stop();
        }

        if (POSTGRES != null) {
            POSTGRES.stop();
        }

        if (STAR_ROCKS != null) {
            STAR_ROCKS.stop();
        }
    }

    private Path getSql(String fileName, Map<String, Object> properties) {
        try {
            Path file = Paths.get(KafkaE2EITCase.class.getResource("/flinkSql/" + fileName).toURI());
            return PlaceholderResolver.getDefaultResolver().resolveByMap(file, properties);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private Path getGroupFile(String fileName, Map<String, Object> properties) {
        try {
            Path file = Paths.get(KafkaE2EITCase.class.getResource("/groupFile/" + fileName).toURI());
            return PlaceholderResolver.getDefaultResolver().resolveByMap(file, properties);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private String getCreateStatement(String fileName, Map<String, Object> properties) {
        URL url = Objects.requireNonNull(KafkaE2EITCase.class.getResource("/env/" + fileName));

        try {
            Path file = Paths.get(url.toURI());
            return PlaceholderResolver.getDefaultResolver().resolveByMap(
                    new String(Files.readAllBytes(file), StandardCharsets.UTF_8),
                    properties);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeKafkaTable(String topic) {
        String fileName = "kafka_test_kafka_init.txt";
        int port = KafkaContainer.ZOOKEEPER_PORT;

        Map<String, Object> properties = new HashMap<>();
        properties.put("TOPIC", topic);
        properties.put("ZOOKEEPER_PORT", port);

        try {
            String createKafkaStatement = getCreateStatement(fileName, properties);
            ExecResult result = KAFKA.execInContainer("bash", "-c", createKafkaStatement);
            LOG.info("Create kafka topic: {}, std: {}", createKafkaStatement, result.getStdout());
            if (result.getExitCode() != 0) {
                throw new RuntimeException("Init kafka topic failed. Exit code:" + result.getExitCode());
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeMysqlTable(String fileName, Map<String, Object> properties) {
        try (Connection conn =
                DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                Statement stat = conn.createStatement()) {
            String createMysqlStatement = getCreateStatement(fileName, properties);
            stat.execute(createMysqlStatement);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void initializePostgresTable() {
        try {
            Class.forName(POSTGRES.getDriverClassName());
            Connection conn = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                    POSTGRES.getPassword());
            Statement stat = conn.createStatement();
            stat.execute(
                    "CREATE TABLE test_input (\n"
                            + "  id SERIAL,\n"
                            + "  name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                            + "  description VARCHAR(512),\n"
                            + "  PRIMARY  KEY(id)\n"
                            + ");");
            // stat.execute("ALTER TABLE test_input REPLICA IDENTITY FULL; ");
            stat.close();
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initializeStarRocksTable() {
        try (Connection conn = DriverManager.getConnection(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(),
                STAR_ROCKS.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute("CREATE TABLE IF NOT EXISTS test_output (\n"
                    + "       id INT NOT NULL,\n"
                    + "       name VARCHAR(255) NOT NULL DEFAULT 'flink',\n"
                    + "       description VARCHAR(512)\n"
                    + ")\n"
                    + "PRIMARY KEY(id)\n"
                    + "DISTRIBUTED by HASH(id) PROPERTIES (\"replication_num\" = \"1\");");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Test flink sql mysql cdc to hive
     *
     * @throws Exception The exception may throw when execute the case
     */
    @Test
    public void testKafkaWithSqlFile() throws Exception {
        final String topic = "test-topic";
        initializeKafkaTable(topic);

        String sqlFile = getSql("kafka_test.sql", new HashMap<>()).toString();
        submitSQLJob(sqlFile, kafkaJar, starrocksJar, postgresJar, mysqlJdbcJar);
        waitUntilJobRunning(Duration.ofSeconds(30));

        // generate input
        try (Connection conn =
                DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                Statement stat = conn.createStatement()) {
            stat.execute(
                    "INSERT INTO test_input VALUES (1,'jacket','water resistant white wind breaker');");
            stat.execute(
                    "INSERT INTO test_input VALUES (2,'scooter','Big 2-wheel scooter ');");
        } catch (SQLException e) {
            LOG.error("Update table for CDC failed.", e);
            throw e;
        }

        JdbcProxy proxy = new JdbcProxy(STAR_ROCKS.getJdbcUrl(), STAR_ROCKS.getUsername(), STAR_ROCKS.getPassword(),
                STAR_ROCKS.getDriverClassName());
        List<String> expectResult = Arrays.asList(
                "1,jacket,water resistant white wind breaker",
                "2,scooter,Big 2-wheel scooter ");
        proxy.checkResultWithTimeout(
                expectResult,
                "test_output",
                3,
                60000L);
    }
}
