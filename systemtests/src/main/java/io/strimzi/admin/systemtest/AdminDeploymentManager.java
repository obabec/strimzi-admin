/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class AdminDeploymentManager {

    private static DockerClient client;
    private static String adminContId;

    public AdminDeploymentManager() {
        client = DockerClientBuilder.getInstance(DefaultDockerClientConfig.createDefaultConfigBuilder().build()).build();
    }

    private void waitForAdminReady() throws TimeoutException, InterruptedException {
        final int waitTimeout = 10;
        Vertx vertx = Vertx.vertx();
        int attempts = 0;
        AtomicBoolean ready = new AtomicBoolean(false);
        while (attempts++ < waitTimeout && !ready.get()) {
            HttpClient client = vertx.createHttpClient();

            client.request(HttpMethod.GET, 8080, "localhost", "/health/status")
                    .compose(req -> req.send().compose(HttpClientResponse::body))
                    .onComplete(httpClientRequestAsyncResult -> {
                        if (httpClientRequestAsyncResult.succeeded()
                                && httpClientRequestAsyncResult.result().toString().equals("{\"status\": \"OK\"}")) {
                            ready.set(true);
                        }
                    });
            Thread.sleep(1000);
        }
        if (!ready.get()) {
            throw new TimeoutException();
        }
    }

    public void deployAdminContainer(String networkId, String kafkaIP) throws Exception {
        ExposedPort adminPort = ExposedPort.tcp(8080);
        Ports portBind = new Ports();
        portBind.bind(adminPort, Ports.Binding.bindPort(8080));

        CreateContainerResponse contResp = client.createContainerCmd("strimzi-admin")
                .withExposedPorts(new ExposedPort(8080))
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind))
                .withCmd("/opt/strimzi/run.sh -e KAFKA_ADMIN_BOOTSTRAP_SERVERS='" + kafkaIP + ":9092' -e KAFKA_ADMIN_OAUTH_ENABLED='false' -e VERTXWEB_ENVIRONMENT='dev'").exec();
        adminContId = contResp.getId();
        client.startContainerCmd(contResp.getId()).exec();
        client.connectToNetworkCmd().withNetworkId(networkId).withContainerId(contResp.getId()).exec();
        waitForAdminReady();
    }

    public void teardown() {
        client.stopContainerCmd(adminContId).exec();
        client.removeContainerCmd(adminContId).exec();
    }
}
