/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class RestOAuthTestIT {
    protected static final Logger LOGGER = LogManager.getLogger(RestOAuthTestIT.class);
    protected static final AdminDeploymentManager DEPLOYMENT_MANAGER = new AdminDeploymentManager();
    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();
    private static TokenModel token = new TokenModel();
    private AdminClient kafkaClient = null;

    @BeforeEach
    public void startup() throws Exception {
        DEPLOYMENT_MANAGER.createNetwork();
        DEPLOYMENT_MANAGER.deployKeycloak();
        DEPLOYMENT_MANAGER.deployZookeeper();
        DEPLOYMENT_MANAGER.deployKafka();
        DEPLOYMENT_MANAGER.deployAdminContainer(DEPLOYMENT_MANAGER.getKafkaIP(), true);

        // Get valid auth token
        Vertx vertx = Vertx.vertx();
        HttpClient client = vertx.createHttpClient();
        String payload = "grant_type=client_credentials&client_secret=kafka-producer-client-secret&client_id=kafka-producer-client";
        CountDownLatch countDownLatch = new CountDownLatch(1);
        client.request(HttpMethod.POST, 8080, "localhost", "/auth/realms/demo/protocol/openid-connect/token")
                .compose(req -> req.putHeader("Host", "keycloak:8080")
                        .putHeader("Content-Type", "application/x-www-form-urlencoded").send(payload))
                .compose(HttpClientResponse::body).onComplete(buffer -> {
                    try {
                        token = new ObjectMapper().readValue(buffer.result().toString(), TokenModel.class);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    LOGGER.warn("Got token");
                    countDownLatch.countDown();
                });
        countDownLatch.await(30, TimeUnit.SECONDS);
        createKafkaAdmin();
    }

    private void createKafkaAdmin() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "30000");
        props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,  "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required oauth.access.token=\"" + token.getAccessToken() + "\";");
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000");
        kafkaClient = KafkaAdminClient.create(props);
    }

    @AfterEach
    public void teardown() {
        kafkaClient.close();
        kafkaClient = null;
        DEPLOYMENT_MANAGER.teardownOauth();
    }

    @Test
    public void listWithValidTokenTest(Vertx vertx, VertxTestContext testContext) throws Exception {
        LOGGER.warn("testino");
        kafkaClient.createTopics(Arrays.asList(
                new NewTopic("test-topic1", 1, (short) 1),
                new NewTopic("test-topic2", 1, (short) 1)
        ));
        DynamicWait.waitForTopicsExists(Arrays.asList("test-topic1", "test-topic2"), kafkaClient);
        HttpClient client = vertx.createHttpClient();
        client.request(HttpMethod.GET, 8081, "localhost", "/rest/topics")
                .compose(req -> req.putHeader("Authorization", "Bearer " + token.getAccessToken()).send().onSuccess(response -> {
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
    public void listWithInvalidTokenTest(Vertx vertx, VertxTestContext testContext) throws InterruptedException {
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
}
