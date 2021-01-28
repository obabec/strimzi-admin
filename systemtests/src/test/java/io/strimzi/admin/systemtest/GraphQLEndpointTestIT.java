/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vimalselvam.graphql.GraphqlTemplate;
import io.strimzi.admin.kafka.admin.model.Types;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphQLEndpointTestIT extends TestBase {
    @Test
    void topicListAfterCreationQLTest(Vertx vertx, VertxTestContext testContext) throws Exception {
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
                    Set<String> namesSet = StreamSupport.stream(jsonNode.get("data").get("topicList")
                            .get("items").spliterator(), false).map(json -> json.get("name").asText()).collect(Collectors.toSet());
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(namesSet).containsAll(actualRestNames);
                    testContext.completeNow();
                })));
    }

    @Test
    void testCreateTopicQL(Vertx vertx, VertxTestContext testContext) throws Exception {
        InputStream iStream = getClass().getResourceAsStream("/graphql/createTopic.graphql");
        ObjectNode variables = new ObjectMapper().createObjectNode();

        Types.NewTopic topic = new Types.NewTopic();
        topic.setName("test-topic");
        topic.setNumPartitions(3);
        topic.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));
        variables.putObject("input");
        variables.set("input", new ObjectMapper().valueToTree(topic));


        String payload = GraphqlTemplate.parseGraphql(iStream, variables);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.POST, 8080, "localhost", "/graphql")
                .compose(req -> req.send(payload).onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicExists(topic.getName(), kafkaClient);
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(actualRestNames).contains(topic.getName());
                    testContext.completeNow();
                })));
    }

    @Test
    void testCreateDuplicatedTopicQL(Vertx vertx, VertxTestContext testContext) throws Exception {
        InputStream iStream = getClass().getResourceAsStream("/graphql/createTopic.graphql");
        ObjectNode variables = new ObjectMapper().createObjectNode();

        Types.NewTopic topic = new Types.NewTopic();
        topic.setName("test-topic");
        topic.setNumPartitions(3);
        topic.setReplicationFactor(1);
        Types.NewTopicConfigEntry config = new Types.NewTopicConfigEntry();
        config.setKey("min.insync.replicas");
        config.setValue("1");
        topic.setConfig(Collections.singletonList(config));
        variables.putObject("input");
        variables.set("input", new ObjectMapper().valueToTree(topic));

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topic.getName(), 1, (short) 1)
        ));

        String payload = GraphqlTemplate.parseGraphql(iStream, variables);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.POST, 8080, "localhost", "/graphql")
                .compose(req -> req.send(payload).onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(actualRestNames).contains(topic.getName());
                    testContext.completeNow();
                })));
    }

    @Test
    void testDeleteTopicQL(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic2";
        InputStream iStream = getClass().getResourceAsStream("/graphql/deleteTopic.graphql");
        ObjectNode variables = new ObjectMapper().createObjectNode();

        variables.putObject("names");
        variables.set("names", new ObjectMapper().valueToTree(Collections.singletonList(topicName)));
        String payload = GraphqlTemplate.parseGraphql(iStream, variables);
        HttpClient client = vertx.createHttpClient();

        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));
        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        client.request(HttpMethod.POST, 8080, "localhost", "/graphql")
                .compose(req -> req.send(payload).onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    DynamicWait.waitForTopicToBeDeleted(topicName, kafkaClient);
                    Set<String> actualRestNames = kafkaClient.listTopics().names().get();
                    assertThat(actualRestNames).doesNotContain(topicName);
                    testContext.completeNow();
                })));
    }

    @Test
    void testUpdateTopicQL(Vertx vertx, VertxTestContext testContext) throws Exception {
        final String topicName = "test-topic-upd";
        InputStream iStream = getClass().getResourceAsStream("/graphql/updateTopic.graphql");
        ObjectNode variables = new ObjectMapper().createObjectNode();
        Types.UpdatedTopic updatedTopic = new Types.UpdatedTopic();
        updatedTopic.setName(topicName);
        Types.NewTopicConfigEntry topicConfigEntry = new Types.NewTopicConfigEntry();
        topicConfigEntry.setKey("max.message.bytes");
        topicConfigEntry.setValue("1041234");
        updatedTopic.setConfig(Collections.singletonList(topicConfigEntry));
        kafkaClient.createTopics(Collections.singletonList(
                new NewTopic(topicName, 2, (short) 1)
        ));

        variables.putObject("input");
        variables.set("input", new ObjectMapper().valueToTree(updatedTopic));
        String payload = GraphqlTemplate.parseGraphql(iStream, variables);
        HttpClient client = vertx.createHttpClient();

        DynamicWait.waitForTopicExists(topicName, kafkaClient);
        client.request(HttpMethod.POST, 8080, "localhost", "/graphql")
                .compose(req -> req.send(payload).onSuccess(response -> {
                    if (response.statusCode() != 200) {
                        testContext.failNow("Status code not correct");
                    }
                }).onFailure(testContext::failNow).compose(HttpClientResponse::body))
                .onComplete(testContext.succeeding(buffer -> testContext.verify(() -> {
                    ConfigResource resource = new ConfigResource(org.apache.kafka.common.config.ConfigResource.Type.TOPIC,
                            topicName);
                    String configVal = kafkaClient.describeConfigs(Collections.singletonList(resource))
                            .all().get().get(resource).get(topicConfigEntry.getKey()).value();
                    assertThat(configVal).isEqualTo(topicConfigEntry.getValue());
                    testContext.completeNow();
                })));

    }

}