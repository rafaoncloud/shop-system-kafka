package com.shop;

import com.costumer.kafka.MyReplyProducer;
import com.data.Item;
import com.data.ItemTransactions;
import com.shop.kafka.ReorderProducer;
import com.shop.kafka.PurchasesConsumer;
import com.shop.kafka.ShipmentsConsumer;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Shop {

    public static final ItemTransactions dbItem = ItemTransactions.getInstance();

    public static final MyReplyProducer replyProducer = MyReplyProducer.getInstance();
    public static final ReorderProducer reorderProducer = ReorderProducer.getInstance();


    public static void main(String[] args) {
        //populateDatabaseWithItens();
        shop();
    }

    public static void shop(){
        Thread purchasesAndReplyThread = handlePurchasesAndReply();
        Thread reorderAndShipmentsThread =  handleReorderAndShipments();
    }

    public static Thread handlePurchasesAndReply(){
        PurchasesConsumer purchasesConsumer = new PurchasesConsumer(replyProducer);
        Thread purchasesAndReplyThread = new Thread(purchasesConsumer);
        purchasesAndReplyThread.start();
        return purchasesAndReplyThread;

        //try {
        //    for(int i = 0; i < 25; i++){
        //        replyProducer.send("Bla Bla");
        //    }
        //    System.out.println("Reply Sent!");
        //} catch (Exception e) {
        //    e.printStackTrace();
        //}
    }

    public static Thread handleReorderAndShipments(){
        ShipmentsConsumer shipmentsConsumer = new ShipmentsConsumer(reorderProducer);
        Thread reorderAndShipmentsThread = new Thread(shipmentsConsumer);
        reorderAndShipmentsThread.start();
        return reorderAndShipmentsThread;
    }

    public static List<Item> getProductsFromDatabase() {
        return dbItem.listItems();
    }

    public static int randomNumber(int bound) {
        return ThreadLocalRandom.current().nextInt(0, bound);
    }

    public static void populateDatabaseWithItens(){
        dbItem.addItem("Patato",10);
        dbItem.addItem("Orange", 22);
        dbItem.addItem("Apple", 23);
        dbItem.addItem("Peach", 29);
        dbItem.addItem("Cabbage", 6);
        dbItem.addItem("Carrot", 8);
        dbItem.addItem("Onion", 12);
        dbItem.addItem("Garlic", 18);
        dbItem.addItem("Pork", 35);
        dbItem.addItem("Beef", 25);
    }
}
