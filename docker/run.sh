#!/usr/bin/env bash
while getopts e: flag
do
    case "${flag}" in
        e) export ${OPTARG}
          echo ${OPTARG}
          ;;
        *)
          ;;
    esac
done
java -cp ./kafka-admin-${STRIMZI_ADMIN_VERSION}-fat.jar:./health-${STRIMZI_ADMIN_VERSION}-fat.jar:./rest-${STRIMZI_ADMIN_VERSION}-fat.jar:./http-server-${STRIMZI_ADMIN_VERSION}-fat.jar io.strimzi.admin.Main -XX:+ExitOnOutOfMemoryError