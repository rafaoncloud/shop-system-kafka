package com.kafkastreams;

import com.KafkaShop;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public  abstract class ReorderStream {

    public static final String IN_TOPIC = KafkaShop.REORDER_TOPIC_INPUT;
    public static final String OUT_TOPIC = KafkaShop.REORDER_TOPIC_OUTPUT;

    public static void start(){

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaShop.APP_ID_CONFIG);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaShop.SERVER_CONFIG);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Long> lines = builder.stream(IN_TOPIC);

        KTable<String, Long> outlines = lines.groupByKey().reduce((oldval, newval) -> oldval + newval);

        outlines.mapValues((k, v) -> k + " => " + v).toStream()
                .to(OUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
