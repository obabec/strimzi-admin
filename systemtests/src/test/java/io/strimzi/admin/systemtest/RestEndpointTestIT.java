/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import io.strimzi.admin.kafka.admin.model.Types;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class RestEndpointTestIT {
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
    void singleRestTopicListTest(Vertx vertx, VertxTestContext testContext) {
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topicList")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).isEqualTo(actualRestNames);
                    testContext.completeNow();
                })));
    }

    @Test
    void topicListAfterCreationTest(Vertx vertx, VertxTestContext testContext) {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
                ));
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topicList")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).isEqualTo(actualRestNames);
                    testContext.completeNow();
                })));
    }

    @Test
    void describeSingleTopic(Vertx vertx, VertxTestContext testContext) throws Exception {
        String topicName = "test-topic1";
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));

        DynamicWait.waitFor(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                if (kafkaClient.listTopics().names().get().contains(topicName)) {
                    return Boolean.TRUE;
                }
                return Boolean.FALSE;
            }
        }, Boolean.TRUE, 10);

        String queryReq = "/rest/topic?name=test-topic1";
        vertx.createHttpClient().request(HttpMethod.GET, 8080, "localhost", queryReq)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Types.Topic topic = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.Topic.class);
                    assertThat(topic.getPartitions().size()).isEqualTo(2);
                    testContext.completeNow();
                })));
    }



}
