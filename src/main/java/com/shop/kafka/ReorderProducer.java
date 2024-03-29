package com.shop.kafka;

import com.KafkaShop;
import com.costumer.kafka.MyReplyProducer;
import com.data.Item;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class ReorderProducer {

    private static ReorderProducer single_instance = null;

    public static final String TOPIC = KafkaShop.REORDER_TOPIC;
    public static final String KEY = "item";
    private static Producer<String,String> producer;

    private ReorderProducer(){
        setUp();
    }

    public static ReorderProducer getInstance() {
        if (single_instance == null)
            single_instance = new ReorderProducer();

        return single_instance;
    }

    public void send(Item item) throws Exception{

        if(single_instance == null)
            throw new RuntimeException("Instance not created!!!");

        try {
            producer.send(new ProducerRecord<String,String>(TOPIC,item.getName(),KafkaShop.serializeItemToJSON(item)));
            producer.flush();
        }catch (Exception e){
            throw e;
        }
    }

    public Producer<String,String> getProducer(){
        return producer;
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
}
