package com.shop.kafka;

import com.KafkaShop;
import com.costumer.kafka.MyReplyProducer;
import com.data.BalanceTransactions;
import com.data.Item;
import com.data.ItemTransactions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Arrays;
import java.util.Properties;

public class PurchasesConsumer implements Runnable {

    public static final String TOPIC = KafkaShop.PURCHASES_TOPIC;
    public static final String KEY = "item";
    public static final String GROUP_ID = "1";

    private final MyReplyProducer replyProducer;

    private final ItemTransactions dbItem;
    private final BalanceTransactions dbBalance;

    public PurchasesConsumer(MyReplyProducer replyProducer, ItemTransactions dbItem, BalanceTransactions dbBalance){
        this.replyProducer = replyProducer;
        this.dbItem = dbItem;
        this.dbBalance = dbBalance;
    }

    @Override
    public void run() {
        KafkaConsumer <String,String> consumer = setUp();

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("[Shop - Purchases] Subscribed to topic " + TOPIC);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.printf("[Shop - Purchases] offset = %d, key = %s, value = %s\n",
                //        record.offset(), record.key(), record.value());
                try {
                    Item costumerRequest = KafkaShop.deserializeItemFromJSON(record.value());
                    int costumerId = costumerRequest.getPrice();
                    // REPLY
                    Item item = handlePurchase(costumerRequest, costumerId);

                    replyProducer.send(item,costumerId);

                    if (item.getPrice() <= 0) {
                        System.out.println("[Reply] Shop refuses the order and provided " + item.getAmount() + " " + item.getName() +
                                " | Balance(" + dbBalance.getBalance().getBalance() + ").");

                    } else {
                        System.out.println("[Reply] Shop accepts the order and provided " + item.getAmount() + " " + item.getName() +
                                " with a cost of " + item.getAmount() * item.getPrice() + " | Balance(" + dbBalance.getBalance().getBalance() + ").");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private Item handlePurchase(final Item costumerRequest,final int costumerId) throws Exception {
        String itemName = costumerRequest.getName();
        int amountToSell = costumerRequest.getAmount();
        Item inDbItem = dbItem.getItem(itemName);
        int totalCost = amountToSell * inDbItem.getPrice();

        // FAIL - Not enough items - amount to sell
        if(amountToSell > inDbItem.getAmount()){
            costumerRequest.setPrice(-1000);
            return costumerRequest;
        }

        // SUCESS - The shop as the requested amount
        dbBalance.balanceIncrease(totalCost);
        dbItem.decreaseItemAmount(itemName,amountToSell);
        costumerRequest.setPrice(inDbItem.getPrice());
        return costumerRequest;
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
