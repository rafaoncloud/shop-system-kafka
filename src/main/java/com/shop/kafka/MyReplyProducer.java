package com.costumer.kafka;

import com.KafkaShop;
import com.data.ItemTransactions;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class MyReplyProducer {

    private static MyReplyProducer single_instance = null;

    public static final String TOPIC = KafkaShop.MY_REPLY_TOPIC;
    public static final String KEY = "reply";
    private static Producer<String,String> producer;

    private MyReplyProducer(){
        setUp();
    }

    public static MyReplyProducer getInstance() {
        if (single_instance == null)
            single_instance = new MyReplyProducer();

        return single_instance;
    }

    private void setUp(){
        Properties props = new Properties();
        //props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaShop.APP_ID_CONFIG);
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaShop.SERVER_CONFIG);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String,String>(props);
    }

    public void send(String replyMessage) throws Exception{

        if(single_instance == null)
            throw new RuntimeException("Instance not created!!!");

        try {
            producer.send(new ProducerRecord<String,String>(TOPIC,KEY,replyMessage));
            producer.flush();
        }catch (Exception e){
            throw e;
        }
    }
}
