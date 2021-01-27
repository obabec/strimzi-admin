package io.strimzi.admin.systemtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vimalselvam.graphql.GraphqlTemplate;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class GraphQLEndpointTestIT {
    protected static final Logger LOGGER = LogManager.getLogger(RestEndpointTestIT.class);

    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    private static Network network;
    private static final AdminDeploymentManager DEPLOYMENT_MANAGER = new AdminDeploymentManager();
    private static AdminClient kafkaClient;
    private static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();

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

    @Test
    void singleGraphQLTopicListTest(Vertx vertx, VertxTestContext testContext) throws Exception {
        InputStream iStream = getClass().getResourceAsStream("/graphql/topicList.graphql");
        String payload = GraphqlTemplate.parseGraphql(iStream, null);
        HttpClient client = vertx.createHttpClient();
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(Arrays.asList("test-topic1", "test-topic2"), kafkaClient);
        client.request(HttpMethod.POST, 8080, "localhost", "/graphql")
                .compose(req -> req.send(payload).onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    JsonNode jsonNode = new ObjectMapper().readTree(buffer.toString());
                    assertThat(jsonNode.get("data").get("topicList").get("items").size()).isEqualTo(3);
                    testContext.completeNow();
                })));
    }
}