/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.kafka.admin.handlers;

import io.strimzi.admin.kafka.admin.TopicOperations;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.graphql.VertxDataFetcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TopicsDeleteHandler extends CommonHandler {
    protected static final Logger log = LogManager.getLogger(TopicsDeleteHandler.class);

    public static VertxDataFetcher deleteTopicsFetcher(Map<String, Object> acConfig, Vertx vertx) {
        VertxDataFetcher<List<String>> dataFetcher = new VertxDataFetcher<>((environment, prom) -> {
            setOAuthToken(acConfig, environment.getContext());

            List<String> topicsToDelete = environment.getArgument("names");
            createAdminClient(vertx, acConfig).onComplete(ac -> {
                if (ac.failed()) {
                    prom.fail(ac.cause());
                } else {
                    TopicOperations.deleteTopics(ac.result(), topicsToDelete, prom);
                }
            });
        });
        return dataFetcher;
    }

    public static Handler<RoutingContext> deleteTopicsHandler(Map<String, Object> acConfig, Vertx vertx) {
        return routingContext -> {
            setOAuthToken(acConfig, routingContext);
            List<String> topicsToDelete = Arrays.asList(routingContext.queryParams().get("names").split(",").clone());
            Promise<List<String>> prom = Promise.promise();
            createAdminClient(vertx, acConfig).onComplete(ac -> {
                if (ac.failed()) {
                    prom.fail(ac.cause());
                } else {
                    TopicOperations.deleteTopics(ac.result(), topicsToDelete, prom);
                    processResponse(prom, routingContext);
                }
            });
        };
    }
}
