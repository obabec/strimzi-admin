/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.admin.kafka.admin.model.Types;
import io.vertx.core.buffer.Buffer;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

public class ModelDeserializer {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public ModelDeserializer() {
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public <T> T deserializeResponse(Buffer responseBuffer, Class<T> clazz) {
        T deserialized = null;
        try {
            deserialized = MAPPER.readValue(responseBuffer.toString(), clazz);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return deserialized;
    }
    public <T> String serializeBody(T object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }


    public Set<String> getNames(Buffer responseBuffer) {
        Set<String> names = null;
        try {
            Types.TopicList topicList = MAPPER.readValue(responseBuffer.toString(), Types.TopicList.class);
            names = new HashSet<>();
            for (Types.Topic topic : topicList.getItems()) {
                names.add(topic.getName());
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return names;
    }

    public Set<String> getNames(Types.TopicList topicList) {
        Set<String> names = new HashSet<>();
        for (Types.Topic topic : topicList.getItems()) {
            names.add(topic.getName());
        }
        return names;
    }
}
