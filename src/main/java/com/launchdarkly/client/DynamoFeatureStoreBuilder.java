package com.launchdarkly.client;

import com.amazonaws.auth.AWSCredentialsProvider;

public class DynamoFeatureStoreBuilder {
    private String region = "us-west-2";
    private AWSCredentialsProvider credentialsProvider;

    public String getRegion() {
        return region;
    }

    public DynamoFeatureStoreBuilder setRegion(String region) {
        this.region = region;
        return this;
    }

    public AWSCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public DynamoFeatureStoreBuilder setCredentialsProvider(AWSCredentialsProvider credentialsProvider) {
        this.credentialsProvider = credentialsProvider;
        return this;
    }

    public DynamoFeatureStore build() {
        return new DynamoFeatureStore(this);
    }
}
