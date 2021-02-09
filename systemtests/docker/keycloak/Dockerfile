FROM quay.io/keycloak/keycloak:12.0.2

ADD ${PWD}/docker/keycloak/scripts /opt/jboss/keycloak/ssl
ADD ${PWD}/docker/target/keycloak/certs /opt/jboss/keycloak/standalone/configuration/certs
ADD ${PWD}/docker/keycloak/realms /opt/jboss/keycloak/realms

ENV KEYCLOAK_USER admin
ENV KEYCLOAK_PASSWORD admin
ENV KEYCLOAK_HTTPS_PORT 8443
ENV PROXY_ADDRESS_FORWARDING true
ENV KEYCLOAK_IMPORT /opt/jboss/keycloak/realms/demo-realm.json
ENTRYPOINT ["/bin/bash", "-c"]
#CMD "ls /opt/jboss/keycloak/standalone/configuration/certs"
CMD ["cd /opt/jboss/keycloak && bin/jboss-cli.sh --file=ssl/keycloak-ssl.cli && rm -rf standalone/configuration/standalone_xml_history/current && cd .. && /opt/jboss/tools/docker-entrypoint.sh  -Dkeycloak.profile.feature.upload_scripts=enabled -b 0.0.0.0"]