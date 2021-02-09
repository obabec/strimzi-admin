package io.strimzi.admin.systemtest;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TokenModel {
    @JsonProperty("access_token")
    private String accessToken;

    @JsonProperty("expires_in")
    private int expire;

    @JsonProperty("refresh_expires_in")
    private int refreshExpire;

    @JsonProperty("token_type")
    private String tokenType;

    @JsonProperty("not-before-policy")
    private String notBeforePolicy;

    @JsonProperty("scope")
    private String scope;

    public TokenModel() {
    }

    public TokenModel(String accessToken, int expire, int refreshExpire, String tokenType, String notBeforePolicy, String scope) {
        this.accessToken = accessToken;
        this.expire = expire;
        this.refreshExpire = refreshExpire;
        this.tokenType = tokenType;
        this.notBeforePolicy = notBeforePolicy;
        this.scope = scope;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public int getExpire() {
        return expire;
    }

    public void setExpire(int expire) {
        this.expire = expire;
    }

    public int getRefreshExpire() {
        return refreshExpire;
    }

    public void setRefreshExpire(int refreshExpire) {
        this.refreshExpire = refreshExpire;
    }

    public String getTokenType() {
        return tokenType;
    }

    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }

    public String getNotBeforePolicy() {
        return notBeforePolicy;
    }

    public void setNotBeforePolicy(String notBeforePolicy) {
        this.notBeforePolicy = notBeforePolicy;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }
}