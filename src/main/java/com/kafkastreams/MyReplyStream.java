package com.kafkastreams;

import com.KafkaShop;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public abstract class MyReplyStream {

    public static final String IN_TOPIC = KafkaShop.MY_REPLY_TOPIC_INPUT;
    public static final String OUT_TOPIC = KafkaShop.MY_REPLY_TOPIC_OUTPUT;

    public static void start(){

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaShop.APP_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaShop.SERVER_CONFIG);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> replies = builder.stream(OUT_TOPIC);

        replies.to(IN_TOPIC);

        //KTable<String, String> repliesTable = replies.
        //        .to(OUT_TOPIC, Produced.with(Serdes.String(),Serdes.String()));

                //replies.groupByKey();
                //.reduce((oldval, newval) -> oldval + newval);

        //outlines.mapValues((k, v) -> k + " => " + v).toStream()
        //        .to(OUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        //outlines.mapValues((k, v) -> " " + v).toStream()
        //        .to(OUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        System.out.println("[Stream] Stream started from " + OUT_TOPIC + " to " + IN_TOPIC + ".");
    }
}
