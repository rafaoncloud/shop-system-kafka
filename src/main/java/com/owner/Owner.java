package com.owner;

import com.costumer.kafka.MyReplyConsumer;
import com.data.Item;
import com.data.ItemTransactions;
import com.owner.kafka.ReorderProducer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Owner {

    public static final ItemTransactions dbItem = ItemTransactions.getInstance();

    public static final ReorderProducer reorderProducer = ReorderProducer.getInstance();

    public static void main(String[] args) {
        randomOwner();
    }

    public static void randomOwner() {
        try {
            while (true) {
                List<Item> items = getProductsFromDatabase();
                int random = randomNumber(items.size() - 1);
                Item item = items.get(random);
                reorderProducer.send(item);
                System.out.println("[Owner] Publish Reorder: " +
                        item.getItemID() + " " +
                        item.getName() + " " +
                        item.getPrice()
                );
                Thread.sleep(500);
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
        return ThreadLocalRandom.current().nextInt(0, bound);
    }
}
