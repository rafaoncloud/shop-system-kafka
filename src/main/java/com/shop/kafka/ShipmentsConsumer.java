package com.shop.kafka;

import com.KafkaShop;
import com.EItem;
import com.data.BalanceTransactions;
import com.data.Item;
import com.data.ItemTransactions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.*;

public class ShipmentsConsumer implements Runnable {

    public static final String TOPIC = KafkaShop.SHIPMENTS_TOPIC;
    public static final String KEY = "item";
    public static final String GROUP_ID = "1";

    private final ReorderProducer reorderProducer;

    private final ItemTransactions itemDb;
    private final BalanceTransactions balanceDb;
    private final int productsRange;

    private final HashMap<String, Item> initialProductsStatusMap; //<name of the item,item>

    public ShipmentsConsumer(ReorderProducer reorderProducer, ItemTransactions itemDb, BalanceTransactions balanceDb,
                             int productsRange) throws IllegalArgumentException {
        if (productsRange <= 0)
            throw new IllegalArgumentException("Products Range must be 1 or higher!");

        this.reorderProducer = reorderProducer;
        this.itemDb = itemDb;
        this.balanceDb = balanceDb;
        this.productsRange = productsRange;
        this.initialProductsStatusMap = new HashMap<>();
    }

    @Override
    public void run() {
        KafkaConsumer<String, String> consumer = setUp();

        consumer.subscribe(Arrays.asList(TOPIC));
        System.out.println("[Shop - Shipments] Subscribed to topic " + TOPIC);

        try {
            define10ItemsInitialValuesAndQuantities(consumer);
            System.out.println("[Shop - Shipments] Shop 10 Initial Items and Quantities" +
                    " received in the shipment, ordered by the Owner" + TOPIC + ".");
            System.out.println("[Shop - Shipments] Shop opens and starts to receive costumer purchases.");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("[Shop - Shipments] offset = %d, key = %s, value = %s\n",
                            record.offset(), record.key(), record.value());

                    // Check Shipment
                    Item item = KafkaShop.deserializeItemFromJSON(record.value());
                    // Pay to the supplier - shipment ignored after 3rd attempt
                    int balance = payToSupplier(item);
                    // Successfully paid to the supplier
                    if (balance > 0) {
                        // Add item to the stock
                        Item itemUpdated = addProductToStock(item);

                        System.out.println("[Shop - Shipments] The shop as a balance of " + balance + "."
                                + " Supplied with " + item.getAmount() + " " + item.getName() + " with a price of " +
                                item.getPrice() + " and being sold by " + itemUpdated.getPrice() + " of " +
                                itemUpdated.getAmount() + ".");
                    } else {
                        // Shipment timeout
                    }
                }
                // Check products stock, if it is low on stock (lower than 25% of the initial value)
                // Constantly checking...
                Item itemLowStock = checkStock();
                if (itemLowStock != null) {
                    reorderProducer.send(itemLowStock);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Item checkStock() throws Exception {
        // Iterate items in the database
        for (EItem enumItem : EItem.values()) {
            Item curProductStock = itemDb.getItem(enumItem.toString());
            Item initialProductStatus = initialProductsStatusMap.get(curProductStock.getName());

            int curAmount = curProductStock.getAmount();
            int initialAmount = initialProductStatus.getAmount();

            // Check if item has a stock below 25% from initial amount
            if (curAmount < (initialAmount / 4) || curAmount == 0) {
                // Clean price from reorder object
                curProductStock.setPrice(0);
                // Amount to reorder 70% of the initial stock e.g., 10 initial -> reorder 7
                curProductStock.setAmount((int) Math.round(initialAmount * 0.7));
                curProductStock.setItemID(null);
                // Product to be reordered
                // Item Low in stock
                System.out.printf("[Shop] The product %s is low on stock - Reorder Units(%d) - Current Stock(%d) - Initial Stock (%d) - Initial Price(%d)\n",
                        curProductStock.getName(), curProductStock.getAmount(), curAmount, initialAmount, initialProductStatus.getPrice());
                return curProductStock;

            }
        }
        return null;
    }

    private int payToSupplier(Item item) throws Exception {
        int shipmentPrice = item.getAmount() * item.getPrice();
        int timeout = 3;
        int ignoreSupply = 3;

        while (true) {
            int balance = balanceDb.getBalance().getBalance();

            if (balance < shipmentPrice) {
                System.out.print("[Shop] Shop don't have enough money to pay the supplier shipment " +
                        "Balance(" + balance + ") Cost(" + shipmentPrice + ").");
                ignoreSupply--;
                if (ignoreSupply > 0) {
                    System.out.println(" I'm waiting " + timeout + " seconds.");
                    Thread.sleep(timeout * 1000);
                    continue;
                } else {
                    System.out.println(" The shipment was ignored after the 3rd attempt to pay.");
                    Thread.sleep(timeout * 1000);
                    return -100000;
                }
            }
            balanceDb.balanceDeduce(shipmentPrice);
            return balance - shipmentPrice;
        }
    }


    private Item addProductToStock(Item itemFromShipment) throws Exception {
        // Add the shipment to the stock
        itemDb.increaseItemAmount(itemFromShipment.getName(), itemFromShipment.getAmount());
        return itemDb.getItem(itemFromShipment.getName());
    }

    private void define10ItemsInitialValuesAndQuantities(KafkaConsumer<String, String> kafkaConsumer) throws Exception {
        int initialProducts = 0;

        System.out.println("[Shop] Waiting for the 10 initial products shipments (Amount & Price)," +
                " ordered by the owner.");
        // Get the first 10 products (price and amount) set by the Owner
        while (initialProducts < productsRange) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.printf("[Shop - Shipments] Product %d - offset = %d, key = %s, value = %s\n",
                //        initialProducts, record.offset(), record.key(), record.value());
                Item item = KafkaShop.deserializeItemFromJSON(record.value());

                // Get the item from the database
                Item itemToUpdate = itemDb.getItem(item.getName());
                // Set the initial amount
                itemToUpdate.setAmount(item.getAmount());
                // Set price with margin of 30% (multiplied by 1.3)
                itemToUpdate.setPrice((int) Math.round(item.getPrice() * KafkaShop.MARGIN));
                // Update the item in the database
                itemDb.updateItem(itemToUpdate.getItemID(), item.getName(), itemToUpdate.getPrice(), itemToUpdate.getAmount());

                System.out.printf("[Shop] Initial Product {%d}: Product(%s) Amount(%d) -- Shipment Price Per Unit(%d) Total(%d) -- Price With Margin(%d) Total (%d)\n",
                        initialProducts + 1,
                        item.getName(), item.getAmount(),
                        item.getPrice(), item.getAmount() * item.getPrice(),
                        itemToUpdate.getPrice(), itemToUpdate.getAmount() * itemToUpdate.getPrice());

                // Map to save the initial products status
                initialProductsStatusMap.put(itemToUpdate.getName(), itemToUpdate);
                initialProducts++;
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
