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
import org.apache.kafka.clients.admin.TopicDescription;
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
import java.util.HashSet;
import java.util.List;
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
        final String TOPIC_NAME = "test-topic1";
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(TOPIC_NAME, 2, (short) 1)
        ));

        DynamicWait.waitForTopicExists(TOPIC_NAME, kafkaClient);

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

    @Test
    void testCreateTopic(Vertx vertx, VertxTestContext testContext) {
        final String TOPIC_NAME = "test-topic3";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(TOPIC_NAME);
        topic.setNumPartitions(3);
        topic.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/createTopic")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code " + response.statusCode() + " is not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicExists(TOPIC_NAME, kafkaClient);
                    TopicDescription description = kafkaClient.describeTopics(Collections.singleton(TOPIC_NAME))
                            .all().get().get(TOPIC_NAME);
                    assertThat(description.isInternal()).isEqualTo(false);
                    assertThat(description.partitions().size()).isEqualTo(3);
                    testContext.completeNow();
                })));

    }

    @Test
    void testCreateFaultTopic(Vertx vertx, VertxTestContext testContext) {
        final String TOPIC_NAME = "test-topic-fail";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(TOPIC_NAME);
        topic.setNumPartitions(3);
        topic.setReplicationFactor(4);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/createTopic")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() != 500) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(TOPIC_NAME);
                    testContext.completeNow();
                })));
    }

    @Test
    void testCreateDuplicatedTopic(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String TOPIC_NAME = "test-topic-dupl";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(TOPIC_NAME);
        topic.setNumPartitions(2);
        topic.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(TOPIC_NAME, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(TOPIC_NAME, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/createTopic")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() != 500) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body));
    }

    @Test
    void testTopicDeleteSingle(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String TOPIC_NAME = "test-topic4";
        String query = "/rest/deleteTopics?names="+TOPIC_NAME;

        kafkaClient.createTopics(Collections.singletonList(
            new NewTopic(TOPIC_NAME, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(TOPIC_NAME, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.DELETE, 8080, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() != 200) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicToBeDeleted(TOPIC_NAME, kafkaClient);
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(TOPIC_NAME);
                    testContext.completeNow();
                })));
    }

    @Test
    void testTopicDeleteMultiple(Vertx vertx, VertxTestContext testContext) throws Exception {
        final List<String> TOPIC_NAMES = Arrays.asList("test-topic5", "test-topic6", "test-topic7");
        String query = "/rest/deleteTopics?names=" + String.join(",", TOPIC_NAMES);
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic(TOPIC_NAMES.get(0), 2, (short) 1),
                new NewTopic(TOPIC_NAMES.get(1), 2, (short) 1),
                new NewTopic(TOPIC_NAMES.get(2), 2, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(TOPIC_NAMES, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.DELETE, 8080, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() != 200) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicsToBeDeleted(TOPIC_NAMES, kafkaClient);
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContainAnyElementsOf(TOPIC_NAMES);
                    testContext.completeNow();
                })));
    }


    }
