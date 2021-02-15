package io.strimzi.admin.systemtest.bases;

import io.strimzi.StrimziKafkaContainer;
import io.strimzi.admin.systemtest.deployment.AdminDeploymentManager;
import io.strimzi.admin.systemtest.json.ModelDeserializer;
import io.strimzi.admin.systemtest.plain.RestEndpointTestIT;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Map;

public class TestBase {
    protected static final Logger LOGGER = LogManager.getLogger(RestEndpointTestIT.class);

    protected static StrimziKafkaContainer kafka = null;

    protected static final AdminDeploymentManager DEPLOYMENT_MANAGER = new AdminDeploymentManager();
    protected static AdminClient kafkaClient;
    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();

    @BeforeEach
    public void startup() throws Exception {
        kafka = new StrimziKafkaContainer();
        kafka.start();
        String networkName = DEPLOYMENT_MANAGER.getNetworkName(kafka.getNetwork().getId());
        String kafkaIp = DEPLOYMENT_MANAGER.getKafkaIP(kafka.getContainerId(), networkName);
        DEPLOYMENT_MANAGER.deployAdminContainer(kafkaIp + ":9093", false, networkName);
        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        conf.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        kafkaClient = AdminClient.create(conf);
    }

    @AfterEach
    public void teardown() {
        LOGGER.info("Teardown docker environment");
        kafkaClient.close();
        kafka.stop();
        kafka = null;
        DEPLOYMENT_MANAGER.teardown();
    }
}
