package com.costumer.kafka;

import com.KafkaShop;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.To;

import java.util.Properties;

public class PurchasesProducer {

    public static final String OUT_TOPIC = KafkaShop.PURCHASES_TOPIC_OUTPUT;

    public static void start(){

    }

}
