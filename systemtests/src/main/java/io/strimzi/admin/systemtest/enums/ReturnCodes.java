/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.admin.systemtest.enums;

public enum ReturnCodes {
    KAFKADOWN(503),
    NOTFOUND(404),
    UNOPER(400),
    SERVERERR(500),
    TOPICCREATED(201),
    DUPLICATED(409),
    UNAUTHORIZED(401),
    SUCC(200);

    public final int code;
    private ReturnCodes(int code) {
        this.code = code;
    }
}
