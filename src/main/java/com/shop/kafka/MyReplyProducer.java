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

    public static final String TOPIC = KafkaShop.MY_REPLY_TOPIC_OUTPUT;
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaShop.APP_ID_CONFIG);

        // kafka bootstrap server
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaShop.SERVER_CONFIG);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // producer acks
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        producer = new KafkaProducer<String,String>(props);
    }

    public void send(String replyMessage) throws Exception{
        try {
            producer.send(replyTransaction(KEY, "Successfully Completed Transaction!"));
        }catch (Exception e){
            throw e;
        }
    }

    public ProducerRecord<String, String> replyTransaction(String key, String replyMessage){
        // creates an empty json {}
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        // { "amount" : 46 } (46 is a random number between 0 and 100 excluded)
        Integer amount = ThreadLocalRandom.current().nextInt(0,100);

        // Instant.now() is to get the current time using Java 8
        Instant now = Instant.now();

        // we write the data to the json document
        transaction.put("reply-message",replyMessage);
        transaction.put("amount",10);
        transaction.put("time", now.toString());
        return new ProducerRecord<>(TOPIC ,key,transaction.toString());
    }
}
