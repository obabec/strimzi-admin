package io.strimzi.admin.systemtest;

public enum ReturnCodes {
    KAFKADOWN(503),
    NOTFOUND(404),
    UNOPER(400),
    TOPICCREATED(201),
    DUPLICATED(409),
    UNAUTHORIZED(401),
    SUCC(200);

    public final int code;
    private ReturnCodes(int code) {
        this.code = code;
    }
}
