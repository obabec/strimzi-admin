package io.strimzi.admin.systemtest;

import io.vertx.junit5.VertxExtension;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

    @BeforeAll
    public static void startup() throws Exception {
        network = Network.newNetwork();
        kafka = kafka.withEmbeddedZookeeper().withNetwork(network);
        kafka.start();
        String kafkaIp = kafka.getContainerInfo().getNetworkSettings().getNetworks()
                .get(((Network.NetworkImpl) network).getName()).getIpAddress();
        DEPLOYMENT_MANAGER.deployAdminContainer(network.getId(), kafkaIp);
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        kafkaClient = AdminClient.create(conf);
    }

    @AfterAll
    public static void teardown() {
        LOGGER.info("Teardown docker environment");
        kafkaClient.close();
        DEPLOYMENT_MANAGER.teardown();
        kafka.stop();
        network.close();
    }
}
