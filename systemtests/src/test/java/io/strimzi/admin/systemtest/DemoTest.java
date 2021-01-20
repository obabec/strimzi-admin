/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;


public class DemoTest {
    protected static final Logger LOGGER = LogManager.getLogger(DemoTest.class);

    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    private static Network network;
    private static AdminDeploymentManager adminDeploymentManager = new AdminDeploymentManager();



    @BeforeAll
    public static void startup() {
        network = Network.newNetwork();
        kafka = kafka.withEmbeddedZookeeper().withNetwork(network);
        kafka.start();
        String kafkaIp = kafka.getContainerInfo().getNetworkSettings().getNetworks().get(((Network.NetworkImpl) network).getName()).getIpAddress();
        adminDeploymentManager.deployAdminContainer(network.getId(), kafkaIp);
    }

    @AfterAll
    public static void teardown() {
        adminDeploymentManager.teardown();
        kafka.stop();
        network.close();
    }

    @Test
    void demoTest() {
        LOGGER.info("est.");
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        AdminClient client = AdminClient.create(conf);

        String serv = kafka.getBootstrapServers();
        LOGGER.info("tesst2.");
    }



}
