prepare-tests:
	cd systemtests/docker/certificates && ls && ./gen-ca.sh && ./gen-keycloak-certs.sh && cd -
	mvn clean install -f systemtests -DskipTests
	docker build ./systemtests -f systemtests/docker/kafka/Dockerfile -t strimzi-admin-kafka
	docker build ./systemtests -f systemtests/docker/keycloak/Dockerfile -t strimzi-admin-keycloak
	docker build ./systemtests -f systemtests/docker/zookeeper/Dockerfile -t strimzi-admin-zookeeper

clean-tests:
	rm -rf ./systemtests/docker/certificates/c*
	rm -rf ./systemtests/docker/certificates/key*
	rm -rf systemtests/docker/target
	docker image rm strimzi-admin-kafka strimzi-admin-keycloak strimzi-admin-zookeeper