package com.supplier;

import com.costumer.kafka.MyReplyConsumer;
import com.costumer.kafka.PurchasesProducer;
import com.data.Item;
import com.data.ItemTransactions;
import com.supplier.kafka.ReorderConsumer;
import com.supplier.kafka.ShipmentsProducer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Supplier {

    public static final ItemTransactions dbItem = ItemTransactions.getInstance();

    public static final ShipmentsProducer shipmentsProducer = ShipmentsProducer.getInstance();

    public static void main(String[] args) {
        randomSupplier();
    }

    public static void randomSupplier() {

        ReorderConsumer reorderConsumer = new ReorderConsumer(shipmentsProducer,10);
        reorderConsumer.run();
    }

    public static List<Item> getProductsFromDatabase() {
        return dbItem.listItems();
    }

    public static int randomNumber(int bound) {
        return ThreadLocalRandom.current().nextInt(0, bound);
    }
}
