package com;

import com.data.Item;
import com.google.gson.Gson;

import java.util.Properties;

public abstract class KafkaShop {

    public static final String APP_ID_CONFIG = "com.shop-system";
    public static final String SERVER_CONFIG = "localhost:9092";

    public static final String MY_REPLY_TOPIC = "my-reply-topic";
    public static final String PURCHASES_TOPIC = "purchases-topic";
    public static final String REORDER_TOPIC = "reorder-topic";
    public static final String SHIPMENTS_TOPIC = "shipments-topic";

    public static final String REPLY_KEY = "reply"; // Value - String
    public static final String ITEM_KEY = "transaction"; // Value - Items

    public static final double MARGIN = 1.3;

    public static final String GROUP_ID = "1";

    public static String serializeItemToJSON(Item item){
        Gson gson = new Gson();
        return gson.toJson(item);
    }

    public static Item deserializeItemFromJSON(String json){
        Gson gson = new Gson();
        return gson.fromJson(json, Item.class);
    }
}
