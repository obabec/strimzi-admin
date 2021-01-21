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

public class AdminDeploymentManager {

    private static DefaultDockerClientConfig defaultDockerClientConfig;
    private static DockerClient client;
    private static String adminContId;

    public AdminDeploymentManager() {
        defaultDockerClientConfig = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        client = DockerClientBuilder.getInstance(defaultDockerClientConfig).build();
    }

    public void deployAdminContainer(String networkId, String kafkaIP) {
        ExposedPort adminPort = ExposedPort.tcp(8080);
        Ports portBind = new Ports();
        portBind.bind(adminPort, Ports.Binding.bindPort(8080));

        CreateContainerResponse contResp = client.createContainerCmd("strimzi-admin")
                .withExposedPorts(new ExposedPort(8080))
                .withHostConfig(new HostConfig()
                        .withPortBindings(portBind))
                .withCmd("/opt/strimzi/run.sh -e KAFKA_ADMIN_BOOTSTRAP_SERVERS='" + kafkaIP + ":9092' -e KAFKA_ADMIN_OAUTH_ENABLED='false'").exec();
        adminContId = contResp.getId();
        client.startContainerCmd(contResp.getId()).exec();
        client.connectToNetworkCmd().withNetworkId(networkId).withContainerId(contResp.getId()).exec();
    }

    public void teardown() {
        client.stopContainerCmd(adminContId).exec();
        client.removeContainerCmd(adminContId).exec();
    }
}
