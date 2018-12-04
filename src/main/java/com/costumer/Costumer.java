package com.costumer;

import com.costumer.kafka.MyReplyConsumer;
import com.costumer.kafka.PurchasesProducer;
import com.data.Item;
import com.data.ItemTransactions;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Costumer {

    public static final ItemTransactions dbItem = ItemTransactions.getInstance();

    public static final PurchasesProducer purchasesProducer = PurchasesProducer.getInstance();

    public static void main(String[] args) {
        randomCostumer();
    }

    public static void randomCostumer() {
        Thread replyConsumerThread = new Thread(new MyReplyConsumer());
        replyConsumerThread.start();

        try {
            while (true) {
                List<Item> items = getProductsFromDatabase();

                int random = randomNumber(items.size() - 1);
                Item item = items.get(random);

                purchasesProducer.send(item);

                System.out.println("[Costumer] Costumer wishes to purchase: " +
                        item.getItemID() + " " +
                        item.getName() + " " +
                        item.getPrice()
                );

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<Item> getProductsFromDatabase() {
        return dbItem.listItems();
    }

    public static int randomNumber(int bound) {
        return ThreadLocalRandom.current().nextInt(0, bound + 1);
    }
}
