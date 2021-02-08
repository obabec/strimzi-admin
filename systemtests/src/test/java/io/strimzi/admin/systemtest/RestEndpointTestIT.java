/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import com.github.dockerjava.api.DockerClient;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class RestEndpointTestIT extends TestBase {

    @Test
    void testTopicListAfterCreation(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topics")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCC.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).containsAll(actualRestNames);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicListWithKafkaDown(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.close();
        HttpClient client = vertx.createHttpClient();
        DEPLOYMENT_MANAGER.getClient().stopContainerCmd(kafka.getContainerId()).exec();

        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topics")
                .compose(req -> req.send().onComplete(l -> testContext.verify(() -> {
                    if (l.succeeded()) {
                        assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKADOWN.code);
                    }
                    testContext.completeNow();
                })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicListWithFilter(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topics?filter=test-topic.*")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCC.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(MODEL_DESERIALIZER.getNames(buffer)).isEqualTo(actualRestNames.stream().filter(name -> name.contains("test-topic")).collect(Collectors.toSet()));
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicListWithFilterNone(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topics?filter=zcfsada.*")
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCC.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(0);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest(name = "testTopicListWithLimit - {0}")
    @ValueSource(ints = {1, 2, 3, 5})
    void testTopicListWithLimit(int limit, Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topics?limit=" + limit)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCC.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(Math.min(limit, 3));
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @ParameterizedTest(name = "testTopicListWithOffset - {0}")
    @ValueSource(ints = {0, 1, 3, 4})
    void testTopicListWithOffset(int offset, Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8080, "localhost", "/rest/topics?offset=" + offset)
                .compose(req -> req.send().onSuccess(response -> {
                    if ((response.statusCode() !=  ReturnCodes.SUCC.code && offset != 4)
                            || (response.statusCode() !=  ReturnCodes.UNOPER.code && offset == 4)) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    if (offset != 4) {
                        assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(3 - offset);
                    }
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeSingleTopic(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic1";
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));

        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        String queryReq = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.GET, 8080, "localhost", queryReq)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.SUCC.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Types.Topic topic = MODEL_DESERIALIZER.deserializeResponse(buffer, Types.Topic.class);
                    assertThat(topic.getPartitions().size()).isEqualTo(2);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeSingleTopicWithKafkaDown(Vertx vertx, VertxTestContext testContext) throws Exception {
        kafkaClient.close();
        final String topicName = "test-topic1";
        String queryReq = "/rest/topics/" + topicName;
        DockerClient client = DEPLOYMENT_MANAGER.getClient();
        client.stopContainerCmd(kafka.getContainerId()).exec();

        vertx.createHttpClient().request(HttpMethod.GET, 8080, "localhost", queryReq)
                .compose(req -> req.send().onComplete(l -> testContext.verify(() -> {
                    if (l.succeeded()) {
                        assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKADOWN.code);
                    }
                    testContext.completeNow();
                })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeNonExistingTopic(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-non-exist";

        String queryReq = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.GET, 8080, "localhost", queryReq)
                .compose(req -> req.send().onSuccess(response -> {
                    if (response.statusCode() !=  ReturnCodes.NOTFOUND.code) {
                        testContext.failNow("Status code not correct");
                    }
                    testContext.completeNow();
                }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopic(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        final String topicName = "test-topic3";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        Types.NewTopicInput topicInput = new Types.NewTopicInput();
        topicInput.setNumPartitions(3);
        topicInput.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topicInput.setConfig(Collections.singletonList(config));
        topic.setSettings(topicInput);

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.TOPICCREATED.code) {
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
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopicWithKafkaDown(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.close();
        final String topicName = "test-topic3";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        Types.NewTopicInput input = new Types.NewTopicInput();
        input.setNumPartitions(3);
        input.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        input.setConfig(Collections.singletonList(config));
        topic.setSettings(input);
        DEPLOYMENT_MANAGER.getClient().stopContainerCmd(kafka.getContainerId()).exec();

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onComplete(l -> testContext.verify(() -> {
                            if (l.succeeded()) {
                                assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKADOWN.code);
                            }
                            testContext.completeNow();
                        })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateWithInvJson(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        final String topicName = "test-topic3";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        Types.NewTopicInput input = new Types.NewTopicInput();
        input.setNumPartitions(3);
        input.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        input.setConfig(Collections.singletonList(config));
        topic.setSettings(input);

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic) + "{./as}").onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.SERVERERR.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopicWithInvName(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        final String topicName = "testTopic3_9-=";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        Types.NewTopicInput input = new Types.NewTopicInput();
        input.setNumPartitions(3);
        input.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        input.setConfig(Collections.singletonList(config));
        topic.setSettings(input);

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.UNOPER.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateFaultTopic(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        final String topicName = "test-topic-fail";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        Types.NewTopicInput input = new Types.NewTopicInput();
        input.setNumPartitions(3);
        input.setReplicationFactor(4);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        input.setConfig(Collections.singletonList(config));
        topic.setSettings(input);

        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.UNOPER.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topicName);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateDuplicatedTopic(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic-dupl";
        Types.NewTopic topic = new Types.NewTopic();
        topic.setName(topicName);
        Types.NewTopicInput input = new Types.NewTopicInput();
        input.setNumPartitions(2);
        input.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        input.setConfig(Collections.singletonList(config));
        topic.setSettings(input);

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.POST, 8080, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.DUPLICATED.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
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
                            if (response.statusCode() !=  ReturnCodes.SUCC.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                        }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicToBeDeleted(topicName, kafkaClient);
                    assertThat(kafkaClient.listTopics().names().get()).doesNotContain(topicName);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicDeleteWithKafkaDown(Vertx vertx, VertxTestContext testContext) throws Exception {
        kafkaClient.close();
        final String topicName = "test-topic4";
        String query = "/rest/topics/" + topicName;
        DEPLOYMENT_MANAGER.getClient().stopContainerCmd(kafka.getContainerId()).exec();

        vertx.createHttpClient().request(HttpMethod.DELETE, 8080, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onComplete(l -> testContext.verify(() -> {
                            if (l.succeeded()) {
                                assertThat(l.result().statusCode()).isEqualTo(ReturnCodes.KAFKADOWN.code);
                            }
                            testContext.completeNow();
                        })).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicDeleteNotExisting(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        final String topicName = "test-topic-non-existing";
        String query = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.DELETE, 8080, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send().onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.NOTFOUND.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

    }

    @Test
    void testUpdateTopic(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
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
                            if (response.statusCode() !=  ReturnCodes.SUCC.code) {
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
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testUpdateTopicWithKafkaDown(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        final String topicName = "test-topic7";
        final String configKey = "min.insync.replicas";
        Types.Topic topic1 = new Types.Topic();
        topic1.setName(topicName);
        Types.ConfigEntry conf = new Types.ConfigEntry();
        conf.setKey(configKey);
        conf.setValue("2");
        topic1.setConfig(Collections.singletonList(conf));
        DEPLOYMENT_MANAGER.getClient().stopContainerCmd(kafka.getContainerId()).exec();

        vertx.createHttpClient().request(HttpMethod.PATCH, 8080, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> {
                            if (response.statusCode() !=  ReturnCodes.KAFKADOWN.code) {
                                testContext.failNow("Status code " + response.statusCode() + " is not correct");
                            }
                            testContext.completeNow();
                        }).onFailure(testContext::failNow));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }
}