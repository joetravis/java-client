package com.launchdarkly.client;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DynamoFeatureStore implements FeatureStore {
    private static final Logger logger = LoggerFactory.getLogger(DynamoFeatureStore.class);
    private static final Gson gson = new Gson();
    private static final String HASH_KEY_NAME = "Key";

    public static final String VALUE_KEY_NAME = "value";

    static DynamoDB dynamoDB;
    private static final String TABLE_NAME = "launchdarkly";

    private boolean initialized = false;

    protected DynamoFeatureStore(DynamoFeatureStoreBuilder builder) {

        AmazonDynamoDB db =
                AmazonDynamoDBClientBuilder.standard()
                .withCredentials(builder.getCredentialsProvider())
                .withRegion(builder.getRegion())
                .build();

        dynamoDB = new DynamoDB(db);
    }

    @Override
    public <T extends VersionedData> T get(VersionedDataKind<T> kind, String key) {
        Table table = getLDTable();
        Item item = table.getItem(HASH_KEY_NAME, key);

        if (item == null) {
            return null;
        }

        return gson.fromJson(item.getJSON(VALUE_KEY_NAME), kind.getItemClass());
    }

    @Override
    public <T extends VersionedData> Map<String, T> all(VersionedDataKind<T> kind) {
        Table table = getLDTable();

        ItemCollection<?> col = table.scan();
        Map<String, T> result = new HashMap<>();

        for(Item item : col) {
             T item1 = gson.fromJson(item.getJSON(VALUE_KEY_NAME), kind.getItemClass());
             result.put(item.getString(HASH_KEY_NAME), item1);
        }

        return result;
    }

    @Override
    public void init(Map<VersionedDataKind<?>, Map<String, ? extends VersionedData>> allData) {
        logger.info("init called");
        Table table = getLDTable();

        for (Map.Entry<VersionedDataKind<?>, Map<String, ? extends VersionedData>> entry: allData.entrySet()) {
            for (VersionedData item: entry.getValue().values()) {
                logger.info("item key: {}", item.getKey());
                Item item1 = new Item()
                        .withPrimaryKey(HASH_KEY_NAME, item.getKey())
                        .withJSON(VALUE_KEY_NAME, gson.toJson(item));
                table.putItem(item1);
            }
        }
        initialized = true;
    }

    @Override
    public <T extends VersionedData> void delete(VersionedDataKind<T> kind, String key, int version) {
        Table table = getLDTable();

        table.deleteItem(HASH_KEY_NAME, key);
    }

    @Override
    public <T extends VersionedData> void upsert(VersionedDataKind<T> kind, T item) {
        Table table = getLDTable();

        Item item1 = new Item()
                .withPrimaryKey(HASH_KEY_NAME, item.getKey())
                .withJSON(VALUE_KEY_NAME, gson.toJson(item));

        table.putItem(item1);
    }

    private Table getLDTable() {
        return dynamoDB.getTable(TABLE_NAME);
    }

    @Override
    public boolean initialized() {
        return initialized;
    }

    @Override
    public void close() throws IOException {

    }
}
