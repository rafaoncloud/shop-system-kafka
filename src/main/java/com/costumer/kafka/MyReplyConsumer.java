package com.costumer.kafka;

import com.KafkaShop;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class MyReplyConsumer {

    public static final String TOPIC = /*KafkaShop.MY_REPLY_TOPIC_INPUT;*/ KafkaShop.MY_REPLY_TOPIC_OUTPUT;
    public static final String KEY = "reply";

    public static final String GROUP_ID = "1";

    public static void run(){

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaShop.APP_ID_CONFIG);
        // kafka bootstrap server
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

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("[Consumer - MyReply] Subscribed to topic " + TOPIC);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(0);
                for (ConsumerRecord<String, String> record : records) {

                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode obj = mapper.readTree(record.value());

                    System.out.printf("offset = %d, key = %s, value = %s, reply = %s\n",
                            record.offset(), record.key(), record.value(), obj.get("reply-message").asText());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
