package com;

import com.costumer.kafka.MyReplyConsumer;
import com.costumer.kafka.MyReplyProducer;
import com.data.ItemTransactions;
import com.kafkastreams.MyReplyStream;

public class Main {

    public static final ItemTransactions itemTransaction = ItemTransactions.getInstance();

    public static void main(String[] args) {
        populateDatabaseWithItens();

        startCostumer();
    }

    public static void populateDatabaseWithItens(){
        itemTransaction.addItem("Patato",10);
        itemTransaction.addItem("Orange", 22);
        itemTransaction.addItem("Apple", 23);
        itemTransaction.addItem("Peach", 29);
        itemTransaction.addItem("Cabbage", 6);
        itemTransaction.addItem("Carrot", 8);
        itemTransaction.addItem("Onion", 12);
        itemTransaction.addItem("Garlic", 18);
        itemTransaction.addItem("Pork", 35);
        itemTransaction.addItem("Beef", 25);
    }

    public static void startCostumer(){

        try {
            MyReplyProducer.getInstance().send("HIIII");
            MyReplyProducer.getInstance().send("HIIII1");
        } catch (Exception e) {
            e.printStackTrace();
        }

        MyReplyStream.start();

        MyReplyConsumer.run();
    }
}
