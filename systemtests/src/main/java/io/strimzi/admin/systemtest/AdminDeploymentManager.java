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
    private static String keycloakContId;
    private static String kafkaContId;
    private static String zookeeperContId;
    private static String networkId;
    protected static final String NETWORK_NAME = "strimzi-admin-network";

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

            client.request(HttpMethod.GET, 8081, "localhost", "/health/status")
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

    private void waitForKeycloakReady() throws TimeoutException, InterruptedException {
        final int waitTimeout = 40;
        Vertx vertx = Vertx.vertx();
        int attempts = 0;
        AtomicBoolean ready = new AtomicBoolean(false);
        while (attempts++ < waitTimeout && !ready.get()) {
            HttpClient client = vertx.createHttpClient();

            client.request(HttpMethod.GET, 8080, "localhost", "/auth/realms/demo")
                    .compose(req -> req.send().onComplete(res -> {
                        if (res.succeeded() && res.result().statusCode() == 200) {
                            ready.set(true);
                        }
                    }));
            Thread.sleep(1000);
        }
        if (!ready.get()) {
            throw new TimeoutException();
        }
    }

    public void deployAdminContainer(String kafkaIP, Boolean oauth) throws Exception {
        ExposedPort adminPort = ExposedPort.tcp(8080);
        Ports portBind = new Ports();
        portBind.bind(adminPort, Ports.Binding.bindPort(8081));

        CreateContainerResponse contResp = client.createContainerCmd("strimzi-admin")
                .withExposedPorts(adminPort)
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind))
                .withCmd("/opt/strimzi/run.sh -e KAFKA_ADMIN_BOOTSTRAP_SERVERS='" + kafkaIP
                        + ":9092' -e KAFKA_ADMIN_OAUTH_ENABLED='" + oauth + "' -e VERTXWEB_ENVIRONMENT='dev'").exec();
        adminContId = contResp.getId();
        client.startContainerCmd(contResp.getId()).exec();
        client.connectToNetworkCmd().withNetworkId(networkId).withContainerId(contResp.getId()).exec();
        waitForAdminReady();
    }

    public void deployKeycloak() throws TimeoutException, InterruptedException {
        ExposedPort port = ExposedPort.tcp(8080);
        ExposedPort portSsl = ExposedPort.tcp(8443);
        Ports portBind = new Ports();
        portBind.bind(port, Ports.Binding.bindPort(8080));
        portBind.bind(portSsl, Ports.Binding.bindPort(8443));
        CreateContainerResponse keycloakResp = client.createContainerCmd("strimzi-admin-keycloak")
                .withExposedPorts(port, portSsl)
                .withName("keycloak")
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind)).exec();
        keycloakContId = keycloakResp.getId();
        client.startContainerCmd(keycloakContId).exec();
        client.connectToNetworkCmd().withNetworkId(networkId).withContainerId(keycloakContId).exec();
        waitForKeycloakReady();
    }

    public void deployZookeeper() {
        ExposedPort port = ExposedPort.tcp(2181);
        Ports portBind = new Ports();
        portBind.bind(port, Ports.Binding.bindPort(2181));
        CreateContainerResponse zookeeperResp = client.createContainerCmd("strimzi-admin-zookeeper")
                .withExposedPorts(port)
                .withName("zookeeper")
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind)).exec();
        zookeeperContId = zookeeperResp.getId();
        client.startContainerCmd(zookeeperContId).exec();
        client.connectToNetworkCmd().withNetworkId(networkId).withContainerId(zookeeperContId).exec();
    }

    public void deployKafka() {
        ExposedPort port = ExposedPort.tcp(9092);
        Ports portBind = new Ports();
        portBind.bind(port, Ports.Binding.bindPort(9092));
        CreateContainerResponse kafkaResp = client.createContainerCmd("strimzi-admin-kafka")
                .withExposedPorts(port)
                .withName("kafka")
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind)).exec();
        kafkaContId = kafkaResp.getId();
        client.startContainerCmd(kafkaContId).exec();
        client.connectToNetworkCmd().withNetworkId(networkId).withContainerId(kafkaContId).exec();
    }

    public String getKafkaIP() {
        return client.inspectContainerCmd(kafkaContId).exec().getNetworkSettings().getNetworks()
                .get(AdminDeploymentManager.NETWORK_NAME).getIpAddress();
    }

    public void createNetwork() {
        networkId = client.createNetworkCmd().withName(NETWORK_NAME).exec().getId();
    }

    public void connectKafkaTestContainerToNetwork(String kafkaContId) {
        client.connectToNetworkCmd().withNetworkId(networkId).withContainerId(kafkaContId).exec();
    }

    public void teardown() {
        client.stopContainerCmd(adminContId).exec();
        client.removeContainerCmd(adminContId).exec();
    }
    //todo: better rewrite this so error states can be handled
    public void teardownOauth() {

        client.stopContainerCmd(adminContId).exec();
        client.removeContainerCmd(adminContId).exec();
        client.stopContainerCmd(kafkaContId).exec();
        client.removeContainerCmd(kafkaContId).exec();
        client.stopContainerCmd(zookeeperContId).exec();
        client.removeContainerCmd(zookeeperContId).exec();
        client.stopContainerCmd(keycloakContId).exec();
        client.removeContainerCmd(keycloakContId).exec();
        client.removeNetworkCmd(networkId).exec();
    }

    public DockerClient getClient() {
        return client;
    }

    public String getAdminContId() {
        return adminContId;
    }
}
