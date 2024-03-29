package com.costumer;

import com.EItem;
import com.KafkaShop;
import com.costumer.kafka.MyReplyConsumer;
import com.costumer.kafka.PurchasesProducer;
import com.data.Item;
import com.data.ItemTransactions;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;

public class Costumer {

    public static final ItemTransactions dbItem = ItemTransactions.getInstance();

    public static final PurchasesProducer purchasesProducer = PurchasesProducer.getInstance();
    public static MyReplyConsumer replyConsumer;

    public static void main(String[] args) {

        if (args.length <= 2) {
            if (args[0].equalsIgnoreCase("auto") && (args[1].equalsIgnoreCase("1") ||
                    args[1].equalsIgnoreCase("2") || args[1].equalsIgnoreCase("3"))) {
                randomCostumer(args[1]);
            } else if (args[0].equalsIgnoreCase("manual") && (args[1].equalsIgnoreCase("1") ||
                    args[1].equalsIgnoreCase("2") || args[1].equalsIgnoreCase("3"))) {
                manual(args[1]);
            }else {
                System.out.println("Illegal Arguments: <auto,manual>");
            }
        } else {
            System.out.println("Illegal Arguments: <auto,manual>");
        }
        System.out.println("Costumer Closed");
    }

    public static void randomCostumer(String costumerId) {
        replyConsumer = new MyReplyConsumer(costumerId);
        Thread readRepliesThread = new Thread(replyConsumer);
        readRepliesThread.start();

        try {
            while (true) {
                List<Item> items = getProductsFromDatabase();

                int random = randomNumber(items.size() - 1);
                Item item = items.get(random);
                item.setItemID(null);
                random = randomNumber(item.getAmount());
                item.setAmount(random);
                int price = item.getPrice();

                // The price number identifies the costumer ID
                item.setPrice(Integer.parseInt(costumerId));
                purchasesProducer.send(item);

                System.out.println("[Random Costumer] Costumer want to buy " + item.getAmount() + " " +
                        item.getName() + " with a cost of " + price*item.getAmount() + ".");

                Thread.sleep(3000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void printItem(int option, Item item){
        System.out.print(option + " - " + item.getName() + ".");
        System.out.print(" Price(" + item.getPrice() + ")");
        System.out.println(" Stock(" + item.getAmount() + ")");
    }

    public static void manual(String costumerId) {
        replyConsumer = new MyReplyConsumer(costumerId);

        Scanner scanner = new Scanner(System.in);
        String input;
        int index, enumCount = 0;

        Item curProductInfo = null;
        Item item = new Item();
        item.setPrice(0);
        String itemName;
        int amount;

        try {
            while (true) {
                System.out.println("Product to reorder:");
                for(EItem enumItem : EItem.values()){
                    enumCount++;
                    try {
                        curProductInfo = dbItem.getItem(enumItem.toString());
                    }catch (Exception e){
                        System.out.println("Shop is not running!");
                        return;
                    }
                    printItem(enumCount,curProductInfo);
                }
                enumCount = 0;
                System.out.println("list - List Again");
                System.out.println("replies - Check and read replies");
                System.out.println("0 - Exit");
                System.out.print("> ");
                input = scanner.nextLine();
                if (input.equalsIgnoreCase("0"))
                    break;
                else if(input.equalsIgnoreCase("list")){
                    continue;
                }else if(input.equalsIgnoreCase("replies")){
                    replyConsumer.receiveReplyQuickCheck(3);
                    continue;
                }
                try {
                    index = Integer.parseInt(input) - 1;
                } catch (NumberFormatException e) {
                    System.out.println("This is not a number in range!");
                    continue;
                }
                itemName = EItem.values()[index].toString();
                curProductInfo = dbItem.getItem(itemName);

                System.out.println("Amount of " + itemName + " to buy: ");
                System.out.print("> ");
                input = scanner.nextLine();
                try {
                    amount = Integer.parseInt(input);
                } catch (NumberFormatException e) {
                    System.out.println("This is not a number in range!");
                    continue;
                }
                item.setName(itemName);
                item.setAmount(amount);
                item.setPrice(0);
                System.out.println("Costumer want to buy " + item.getAmount() + " " +
                        item.getName() + " with a total cost of " + (curProductInfo.getPrice() * item.getAmount()) + " and each "
                                + curProductInfo.getPrice() + ".");
                purchasesProducer.send(item);
                replyConsumer.receiveReply();
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
