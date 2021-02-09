/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import io.vertx.junit5.VertxExtension;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(VertxExtension.class)
public class TestBase {
    protected static final Logger LOGGER = LogManager.getLogger(RestEndpointTestIT.class);

    protected static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    protected static Network network;
    protected static final AdminDeploymentManager DEPLOYMENT_MANAGER = new AdminDeploymentManager();
    protected static AdminClient kafkaClient;
    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();

    @BeforeEach
    public void startup() throws Exception {
        DEPLOYMENT_MANAGER.createNetwork();
        kafka = kafka.withEmbeddedZookeeper();
        kafka.start();
        DEPLOYMENT_MANAGER.connectKafkaTestContainerToNetwork(kafka.getContainerId());
        String kafkaIp = kafka.getContainerInfo().getNetworkSettings().getNetworks()
                .get(AdminDeploymentManager.NETWORK_NAME).getIpAddress();
        DEPLOYMENT_MANAGER.deployAdminContainer(kafkaIp, false);
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        kafkaClient = AdminClient.create(conf);
    }

    @AfterEach
    public void teardown() {
        LOGGER.info("Teardown docker environment");
        kafkaClient.close();
        DEPLOYMENT_MANAGER.teardown();
        kafka.stop();
        network.close();
    }
}
