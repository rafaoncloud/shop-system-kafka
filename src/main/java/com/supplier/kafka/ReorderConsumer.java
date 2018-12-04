package com.supplier.kafka;

import com.KafkaShop;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;

public class ReorderConsumer {

    public static final String TOPIC = KafkaShop.REORDER_TOPIC;
    public static final String KEY = "item";
    public static final String GROUP_ID = "1";


    public void run(ShipmentsProducer shipmentsProducer) {
        KafkaConsumer <String,String> consumer = setUp();

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("[Supplier - Reorder] Subscribed to topic " + TOPIC);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("[Supplier - Reorder] offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());

                try {
                    shipmentsProducer.send(KafkaShop.deserializeItemFromJSON(record.value()));
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }


            }
        }
    }

    private KafkaConsumer<String,String> setUp(){
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
