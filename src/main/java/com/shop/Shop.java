package com.shop;

import com.EItem;
import com.costumer.kafka.MyReplyProducer;
import com.data.BalanceTransactions;
import com.data.Item;
import com.data.ItemTransactions;
import com.shop.kafka.ReorderProducer;
import com.shop.kafka.PurchasesConsumer;
import com.shop.kafka.ShipmentsConsumer;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

public class Shop {

    public static final ItemTransactions dbItem = ItemTransactions.getInstance();
    public static final BalanceTransactions dbBalance = BalanceTransactions.getInstance();

    public static final MyReplyProducer replyProducer = MyReplyProducer.getInstance();
    public static final ReorderProducer reorderProducer = ReorderProducer.getInstance();


    public static void main(String[] args) {
        //populateDatabaseWithItens();
        setUpAsCleanShop(0);
        shop(10);
    }

    public static void shop(int productsRange) {
        Thread purchasesAndReplyThread = handlePurchasesAndReply();
        Thread reorderAndShipmentsThread = handleReorderAndShipments(productsRange);
    }

    public static Thread handlePurchasesAndReply() {
        PurchasesConsumer purchasesConsumer = new PurchasesConsumer(replyProducer, dbItem, dbBalance);
        Thread purchasesAndReplyThread = new Thread(purchasesConsumer);
        purchasesAndReplyThread.start();
        return purchasesAndReplyThread;
    }

    public static Thread handleReorderAndShipments(int productsRange) {
        ShipmentsConsumer shipmentsConsumer = new ShipmentsConsumer(reorderProducer, dbItem, dbBalance, productsRange);
        Thread reorderAndShipmentsThread = new Thread(shipmentsConsumer);
        reorderAndShipmentsThread.start();
        return reorderAndShipmentsThread;
    }

    public static int randomNumber(int bound) {
        return ThreadLocalRandom.current().nextInt(0, bound);
    }

    public static void setUpAsCleanShop(int initialShopBalance) {

        try {
            // Initial shop balance
            dbBalance.updateBalance(initialShopBalance);

            // Item reseted to 0 amount and 0 price
            for (EItem enumItem : EItem.values()) {
                setItemPriceAndAmountZero(enumItem.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean setItemPriceAndAmountZero(String itemName) throws Exception {

        // The item is already described in the database
        Item item = dbItem.getItem(itemName);
        if (item == null) {
            return false;
        }

        item.setPrice(0);
        item.setAmount(0);

        // Update in the database
        dbItem.updateItem(item.getItemID(), item.getName(), item.getPrice(), item.getAmount());
        return true;
    }

    public static void populateDatabaseWithItens() {
        // Drop Tables
        //dbItem.dropTable();
        //dbBalance.dropTable();

        // Add content
        dbBalance.addBalance();

        for (EItem enumItem : EItem.values()) {
            dbItem.addItem(enumItem.toString(), 0, 0);
        }
    }
}
