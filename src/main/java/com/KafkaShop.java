package com;

import java.util.Properties;

public abstract class KafkaShop {

    public static final String APP_ID_CONFIG = "com.shop-system";
    public static final String SERVER_CONFIG = "127.0.0.1:9092";

    public static final String MY_REPLY_TOPIC = "my-reply-topic";
    public static final String PURCHASES_TOPIC = "purchases-topic";
    public static final String REORDER_TOPIC = "reorder-topic";
    public static final String SHIPMENTS_TOPIC = "shipments-topic";

    public static final String MY_REPLY_TOPIC_INPUT = "my-reply-topic-output";
    public static final String PURCHASES_TOPIC_INPUT = "purchases-topic-output";
    public static final String REORDER_TOPIC_INPUT = "reorder-topic-output";
    public static final String SHIPMENTS_TOPIC_INPUT = "shipments-topic-output";

    public static final String MY_REPLY_TOPIC_OUTPUT = "my-reply-topic-input";
    public static final String PURCHASES_TOPIC_OUTPUT = "purchases-topic-input";
    public static final String REORDER_TOPIC_OUTPUT = "reorder-topic-input";
    public static final String SHIPMENTS_TOPIC_OUTPUT = "shipments-topic-input";


    public static final String REPLY_KEY = "reply"; // Value - String
    public static final String ITEM_KEY = "transaction"; // Value - Items

    public static final String GROUP_ID = "1";
}
