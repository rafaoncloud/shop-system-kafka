package com.supplier.kafka;

import com.KafkaShop;
import com.data.Item;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ReorderConsumer {

    public static final String TOPIC = KafkaShop.REORDER_TOPIC;
    public static final String KEY = "item";
    public static final String GROUP_ID = "1";

    private final ShipmentsProducer shipmentsProducer;

    private final int productsRange;
    private final List<Item> initialProductsStatusList;

    public ReorderConsumer(ShipmentsProducer shipmentsProducer, int productsRange) throws IllegalArgumentException {
        if (productsRange <= 0)
            throw new IllegalArgumentException("Products Range must be 1 or higher!");

        this.shipmentsProducer = shipmentsProducer;
        this.productsRange = productsRange;
        this.initialProductsStatusList = new ArrayList<>();
    }

    public void run() {
        KafkaConsumer<String, String> consumer = setUp();

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("[Supplier - Reorder] Subscribed to topic " + TOPIC);
        try {
            define10ItemsInitialValuesAndQuantities(consumer);
            System.out.println("[Supplier - Reorder] Owner 10 Initial Items and Quantities" +
                    " received and shipped to " + TOPIC + ".");
            System.out.println("[Supplier - Reorder] Supplier starts to handle  re-orders and shipments.");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[Supplier - Reorder] offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());

                    Item product = KafkaShop.deserializeItemFromJSON(record.value());
                    for(Item initialItemStatus : initialProductsStatusList){
                        if(initialItemStatus.getName().compareToIgnoreCase(product.getName()) == 0){
                            // Set price to the item with the deduced margin
                            product.setPrice(initialItemStatus.getPrice());
                        }
                    }
                    shipmentsProducer.send(product);
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // This orders came from the owner!
    // The shop is not programmed to send reorders before as the 10 initial products amount & price
    private void define10ItemsInitialValuesAndQuantities(KafkaConsumer<String, String> kafkaConsumer) throws Exception {
        int initialProducts = productsRange;
        System.out.println("[Supplier - Reorder] Waiting for the 10 initial products (Amount & Price) from owner.");
        while (initialProducts > 0) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("[Supplier - Reorder] Initial 10 Items -> offset = %d, key = %s, value = %s\n",
                        record.offset(), record.key(), record.value());
                Item productInitialStatus = KafkaShop.deserializeItemFromJSON(record.value());

                // The product price with a cut margin of 30%
                productInitialStatus.setPrice((int) Math.round(productInitialStatus.getPrice() * 0.70));
                shipmentsProducer.send(productInitialStatus);

                // Add to list
                initialProductsStatusList.add(productInitialStatus);
                initialProducts--;
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
