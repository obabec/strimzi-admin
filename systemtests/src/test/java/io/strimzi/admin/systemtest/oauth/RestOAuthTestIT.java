/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.oauth;

import io.strimzi.admin.kafka.admin.model.Types;
import io.strimzi.admin.systemtest.DynamicWait;
import io.strimzi.admin.systemtest.enums.ReturnCodes;
import io.strimzi.admin.systemtest.bases.OauthTestBase;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class RestOAuthTestIT extends OauthTestBase {
    @Test
    public void testListWithValidToken(Vertx vertx, VertxTestContext testContext) throws Exception {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(Arrays.asList("test-topic1", "test-topic2"), kafkaClient);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8081, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCC.code) {
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
    public void testListUnauthorizedUser(Vertx vertx, VertxTestContext testContext) throws Exception {
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(Arrays.asList("test-topic1", "test-topic2"), kafkaClient);
        changeTokenToUnauthorized(vertx, testContext);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8081, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCC.code) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    assertThat(MODEL_DESERIALIZER.getNames(buffer).size()).isEqualTo(0);
                    testContext.completeNow();
                })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();

    }

    @Test
    public void testListWithInvalidToken(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        kafkaClient.close();
        String invalidToken = new Random().ints(97, 98)
                .limit(token.getAccessToken().length())
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8081, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + invalidToken).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    public void testListWithExpiredToken(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
        // Wait for token to expire
        Thread.sleep(60_000);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8081, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testDescribeSingleTopicAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic1";
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);

        String queryReq = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.GET, 8081, "localhost", queryReq)
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
                    if (response.statusCode() != ReturnCodes.SUCC.code) {
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
    void testDescribeSingleTopicUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic1";
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        changeTokenToUnauthorized(vertx, testContext);

        String queryReq = "/rest/topics/" + topicName;
        vertx.createHttpClient().request(HttpMethod.GET, 8081, "localhost", queryReq)
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send()
                        .onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testCreateTopicAuthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
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

        vertx.createHttpClient().request(HttpMethod.POST, 8081, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
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
    void testCreateTopicUnauthorized(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
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
        changeTokenToUnauthorized(vertx, testContext);
        vertx.createHttpClient().request(HttpMethod.POST, 8081, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(topic)).onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testTopicDeleteSingleAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic4";
        String query = "/rest/topics/" + topicName;

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.DELETE, 8081, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
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
    void testTopicDeleteSingleUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic4";
        String query = "/rest/topics/" + topicName;

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        changeTokenToUnauthorized(vertx, testContext);
        vertx.createHttpClient().request(HttpMethod.DELETE, 8081, "localhost", query)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send().onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }

    @Test
    void testUpdateTopicAuthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
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
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        vertx.createHttpClient().request(HttpMethod.PATCH, 8081, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
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
    void testUpdateTopicUnauthorized(Vertx vertx, VertxTestContext testContext) throws Exception {
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
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        changeTokenToUnauthorized(vertx, testContext);
        vertx.createHttpClient().request(HttpMethod.PATCH, 8081, "localhost", "/rest/topics/" + topicName)
                .compose(req -> req.putHeader("content-type", "application/json")
                        .putHeader("Authorization", "Bearer " + token.getAccessToken())
                        .send(MODEL_DESERIALIZER.serializeBody(topic1)).onSuccess(response -> testContext.verify(() -> {
                            assertThat(response.statusCode()).isEqualTo(ReturnCodes.UNAUTHORIZED.code);
                            testContext.completeNow();
                        })));
        assertThat(testContext.awaitCompletion(1, TimeUnit.MINUTES)).isTrue();
    }
}