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
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class RestEndpointTestIT extends TestBase {

    //todo: topiclist params test
    @Test
    void topicListAfterCreationTest(Vertx vertx, VertxTestContext testContext) {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topics")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).containsAll(actualRestNames);
                    testContext.completeNow();
                })));
    }

    @Test
    void topicListWithFilterTest(Vertx vertx, VertxTestContext testContext) {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topics?filter=test-topic.*")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).isEqualTo(actualRestNames.stream().filter(name -> name.contains("test-topic")).collect(Collectors.toSet()));
                    testContext.completeNow();
                })));
    }

    @Test
    void describeSingleTopic(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic1";
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));

        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        String queryReq = "/rest/topics/" + topicName;
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
    void describeNonExistingTopic(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-non-exist";

        String queryReq = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.GET, 8080, "localhost", queryReq)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() != 404) {
                        testContext.failNow("Status code not correct");
                    }
                    testContext.completeNow();
                }).onFailure(testContext::failNow));
    }

    @Test
    void testCreateTopic(Vertx vertx, VertxTestContext testContext) {
        final String topicName = "test-topic3";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        topic.setNumPartitions(3);
        topic.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() != 201) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicExists(topicName, kafkaClient);
                    TopicDescription description = kafkaClient.describeTopics(Collections.singleton(topicName))
                            .all().get().get(topicName);
                    assertThat(description.isInternal()).isEqualTo(false);
                    assertThat(description.partitions().size()).isEqualTo(3);
                    testContext.completeNow();
                })));

    }

    @Test
    void testCreateWithInvJson(Vertx vertx, VertxTestContext testContext) {
        final String topicName = "test-topic3";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        topic.setNumPartitions(3);
        topic.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic) + "{./as}").onSuccess(response -> {
                            if (response.statusCode() != 400) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body));
    }

    @Test
    void testCreateTopicWithInvName(Vertx vertx, VertxTestContext testContext) {
        final String topicName = "testTopic3_9-=";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        topic.setNumPartitions(3);
        topic.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() != 400) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body));
    }

    @Test
    void testCreateFaultTopic(Vertx vertx, VertxTestContext testContext) {
        final String topicName = "test-topic-fail";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        topic.setNumPartitions(3);
        topic.setReplicationFactor(4);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() != 400) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topicName);
                    testContext.completeNow();
                })));
    }
    //todo: OAUTH, Tests with kafka down
    @Test
    void testCreateDuplicatedTopic(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic-dupl";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        topic.setNumPartitions(2);
        topic.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() != 400) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body));
    }

    @Test
    void testTopicDeleteSingle(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic4";
        String query = "/rest/topics/" + topicName;

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.DELETE, 8080, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() != 200) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicToBeDeleted(topicName, kafkaClient);
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topicName);
                    testContext.completeNow();
                })));
    }

  /*  @Test
    void testTopicDeleteMultiple(Vertx vertx, VertxTestContext testContext) throws Exception {
        final List<String> topicNameS = Arrays.asList("test-topic5", "test-topic6", "test-topic7");
        String query = "/rest/deleteTopics?names=" + String.join(",", topicNameS);
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic(topicNameS.get(0), 2, (short) 1),
                new NewTopic(topicNameS.get(1), 2, (short) 1),
                new NewTopic(topicNameS.get(2), 2, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(topicNameS, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.DELETE, 8080, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() != 200) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicsToBeDeleted(topicNameS, kafkaClient);
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContainAnyElementsOf(topicNameS);
                    testContext.completeNow();
                })));
    }*/

    @Test
    void testTopicDeleteNotExisting(Vertx vertx, VertxTestContext testContext) {
        final String topicName = "test-topic-non-existing";
        String query = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.DELETE, 8080, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() != 404) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body));
    }

    @Test
    void testUpdateTopic(Vertx vertx, VertxTestContext testContext) {
        final String topicName = "test-topic7";
        final String configKey = "min.insync.replicas";
        Types.Topic topic1 = new Types.Topic();
        topic1.setName(topicName);
        Types.ConfigEntry conf = new Types.ConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        topic1.setConfig(Collections.singletonList(conf));

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 1, (short) 1)
        ));

        vertx.createHttpClient().request(HttpMethod.PATCH, 8080, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> {
                            if (response.statusCode() != 200) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicExists(topicName, kafkaClient);
                    ConfigResource resource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,
                            topicName);
                    String configVal = kafkaClient.describeConfigs(Collections.singletonList(resource))
                            .all().get().get(resource).get("min.insync.replicas").value();
                    assertThat(configVal).isEqualTo("2");
                    testContext.completeNow();
                })));

    }
}