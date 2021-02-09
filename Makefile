prepare-tests:
	mvn clean install -f systemtests -DskipTests
	docker build ./systemtests -f systemtests/docker/kafka/Dockerfile -t strimzi-admin-kafka
	docker build ./systemtests -f systemtests/docker/keycloak/Dockerfile -t strimzi-admin-keycloak
	docker build ./systemtests -f systemtests/docker/zookeeper/Dockerfile -t strimzi-admin-zookeeper