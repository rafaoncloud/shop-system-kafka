package com.costumer.kafka;

import com.KafkaShop;
import com.data.Item;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

public class MyReplyConsumer implements Runnable {

    public static final String KEY = "reply";
    public static final String GROUP_ID = "1";

    private final KafkaConsumer<String, String> consumer;
    public final String TOPIC;

    public MyReplyConsumer(String costumerId) {
        this.consumer = setUp();
        TOPIC = KafkaShop.MY_REPLY_TOPIC + costumerId;

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("[Consumer - MyReply] Subscribed to Topic: " + TOPIC);
    }

    public void receiveReply() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.printf("[My Reply] offset = %d, key = %s, value = %s\n",
                //        record.offset(), record.key(), record.value());
                Item item = KafkaShop.deserializeItemFromJSON(record.value());
                if (item.getPrice() <= 0) {
                    System.out.println("[Reply] Shop refuses the order and provided " + item.getAmount() + " " + item.getName());

                } else {
                    System.out.println("[Reply] Shop accepts the order and provided " + item.getAmount() + " " + item.getName() +
                            " with a cost of " + item.getAmount() * item.getPrice());
                }
                return;
            }
        }
    }

    public void receiveReplyQuickCheck(int timeoutSeconds) {
        ConsumerRecords<String, String> records = consumer.poll(timeoutSeconds * 1000);
        for (ConsumerRecord<String, String> record : records) {
            //System.out.printf("[My Reply] offset = %d, key = %s, value = %s\n",
            //        record.offset(), record.key(), record.value());
            Item item = KafkaShop.deserializeItemFromJSON(record.value());
            if (item.getPrice() <= 0) {
                System.out.println("[Reply] Shop refuses the order and provided " + item.getAmount() + " " + item.getName());

            } else {
                System.out.println("[Reply] Shop accepts the order and provided " + item.getAmount() + " " + item.getName() +
                        " with a cost of " + item.getAmount() * item.getPrice());
            }
            return;
        }
    }

    @Override
    public void run() {

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.printf("[My Reply] offset = %d, key = %s, value = %s\n",
                //        record.offset(), record.key(), record.value());
                Item item = KafkaShop.deserializeItemFromJSON(record.value());
                if (item.getPrice() <= 0) {
                    System.out.println("[Reply] Shop refuses the order and provided " + item.getAmount() + " " + item.getName() + ".");

                } else {
                    System.out.println("[Reply] Shop accepts the order and provided " + item.getAmount() + " " + item.getName() +
                            " with a cost of " + item.getAmount() * item.getPrice() + ".");
                }
            }
        }
    }

    private KafkaConsumer<String, String> setUp() {
        Properties props = new Properties();
        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaShop.APP_ID_CONFIG);
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaShop.SERVER_CONFIG);
        props.put("group.id", GROUP_ID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        return consumer;
    }
}
