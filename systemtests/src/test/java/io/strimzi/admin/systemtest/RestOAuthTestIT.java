/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(VertxExtension.class)
public class RestOAuthTestIT {
    protected static final Logger LOGGER = LogManager.getLogger(RestOAuthTestIT.class);
    protected static final AdminDeploymentManager DEPLOYMENT_MANAGER = new AdminDeploymentManager();
    protected static AdminClient kafkaClient;
    protected static final ModelDeserializer MODEL_DESERIALIZER = new ModelDeserializer();
    private static TokenModel token = new TokenModel();

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
        VertxTestContext testContext = new VertxTestContext();
        String payload = "grant_type=client_credentials&client_secret=kafka-broker-secret&client_id=kafka-broker";
        client.request(HttpMethod.POST, 8080, "localhost", "/auth/realms/demo/protocol/openid-connect/token")
                .compose(req -> req.putHeader("Host", "keycloak:8080")
                        .putHeader("Content-Type", "application/x-www-form-urlencoded").send(payload))
                .compose(HttpClientResponse::body).onComplete(buffer -> testContext.verify(() -> {
                    token = new ObjectMapper().readValue(buffer.result().toString(), TokenModel.class);
                    LOGGER.info("xx");
                    testContext.completeNow();
                }));
        assertThat(testContext.awaitCompletion(2, TimeUnit.MINUTES)).isTrue();
    }

    @AfterEach
    public void teardown() {
        DEPLOYMENT_MANAGER.teardownOauth();
    }

    @Test
    public void simplePOCTest(Vertx vertx, VertxTestContext testContext) {
        LOGGER.info("test");


        assertThat(1).isEqualTo(1);
        testContext.completeNow();
    }
}
