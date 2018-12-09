package com.owner;

import com.EItem;
import com.data.Item;
import com.owner.kafka.ReorderProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ThreadLocalRandom;


public class Owner {

    public static final ReorderProducer reorderProducer = ReorderProducer.getInstance();
    public static final List<Item> initialProductsList = itemsToPopulateDatabase();

    /**
     * @param args args[0] - assume an algorithm requesting automatically item to the shop (bot)
     *             args[0] - reorder items through the console (manual)
     *             args[1] - initial -> set the 10 initial items - then open shop
     *             args[1] - continue -> assume control with an already opened show
     */
    public static void main(String[] args) {

        String auto = "auto";
        String initial = "initial";

        if (args.length >= 2) {

            if (args[0].equalsIgnoreCase("auto")) {
                if (args[1].equalsIgnoreCase("initial")) {
                    randomOwner(true);
                } else if (args[1].equalsIgnoreCase("continue")) {
                    randomOwner(false);
                } else {
                    System.out.println("Missing arguments: <auto,manual> <initial,continue>");
                }
            } else if (args[0].equalsIgnoreCase("manual")) {
                if (args[1].equalsIgnoreCase("initial")) {
                    manual(true);
                } else if (args[1].equalsIgnoreCase("continue")) {
                    manual(false);
                } else {
                    System.out.println("Missing arguments: <auto,manual> <initial,continue>");
                }
            }
        } else {
            System.out.println("Missing arguments: <auto,manual> <initial,continue>");
        }

    }

    public static void manual(boolean initial) {
        Scanner scanner = new Scanner(System.in);
        String input;
        int index;

        Item item = new Item();
        item.setPrice(0);
        String itemName;
        int amount;

        try {
            // If intents to populate the shop with the 10 items
            if (initial) {
                send10InitialProducts();
            }
            // Reorders cycle
            while (true) {
                System.out.println("Product to reorder:");
                System.out.println("1 - " + EItem.POTATO.toString());
                System.out.println("2 - " + EItem.ORANGE.toString());
                System.out.println("3 - " + EItem.APPLE.toString());
                System.out.println("4 - " + EItem.PEACH.toString());
                System.out.println("5 - " + EItem.CABBAGE.toString());
                System.out.println("6 - " + EItem.CARROT.toString());
                System.out.println("7 - " + EItem.ONION.toString());
                System.out.println("8 - " + EItem.GARLIC.toString());
                System.out.println("9 - " + EItem.PORK.toString());
                System.out.println("10 - " + EItem.BEEF.toString());
                System.out.println("0 - Exit");
                System.out.print("> ");
                input = scanner.nextLine();
                if (input.compareToIgnoreCase("0") == 0)
                    break;
                try {
                    index = Integer.parseInt(input) - 1;
                } catch (NumberFormatException e) {
                    System.out.println("This is not a number in range!");
                    continue;
                }
                itemName = EItem.values()[index].toString();

                System.out.println("Amount (0-20): ");
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

                reorderProducer.send(item);
                printItem(item, false);
            }

            System.out.println("[Owner] Owner closed.");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void randomOwner(boolean initial) {
        try {
            // If intents to populate the shop with the 10 items
            if (initial) {
                send10InitialProducts();
            }
            // Reorders cycle
            while (true) {

                int random = randomNumber(initialProductsList.size() - 1);
                Item item = initialProductsList.get(random);
                item.setPrice(0);
                random = randomNumber(10);
                item.setAmount(random);

                reorderProducer.send(item);
                printItem(item, false);

                // Interval 10-40 seconds
                random = 10000 + randomNumber(30000);
                Thread.sleep(random);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void send10InitialProducts() throws Exception {
        System.out.println("[Owner] Initial Items 10 to set up");
        // Set Up the Shop with 10 products
        for (Item initialItem : initialProductsList) {
            reorderProducer.send(initialItem);
            printItem(initialItem, true);
        }
        System.out.println("[Owner] The shop was successfully initialized with 10 items.");
    }

    public static void printItem(Item item, boolean showPrice) {
        System.out.print("[Owner] Publish Reorder:" +
                " Name(" + item.getName() + ")");
        if (showPrice) {
            System.out.print(" Price(" + item.getPrice() + ")");
        }
        System.out.println(" Amount(" + item.getAmount() + ").");
    }

    public static List<Item> itemsToPopulateDatabase() {
        List<Item> items = new ArrayList<>();
        items.add(new Item(null, EItem.POTATO.toString(), 10, 10));
        items.add(new Item(null, EItem.ORANGE.toString(), 22, 20));
        items.add(new Item(null, EItem.APPLE.toString(), 23, 50));
        items.add(new Item(null, EItem.PEACH.toString(), 29, 50));
        items.add(new Item(null, EItem.CABBAGE.toString(), 6, 20));
        items.add(new Item(null, EItem.CARROT.toString(), 8, 10));
        items.add(new Item(null, EItem.ONION.toString(), 12, 50));
        items.add(new Item(null, EItem.GARLIC.toString(), 18, 10));
        items.add(new Item(null, EItem.PORK.toString(), 35, 50));
        items.add(new Item(null, EItem.BEEF.toString(), 25, 30));

        // Randomize items
        for (Item item : items) {
            int random = randomNumber(10);
            item.setPrice(item.getPrice() + random);
            int amountRandom = randomNumber(100) + 10;
            item.setAmount(amountRandom);
        }

        return items;
    }

    public static int randomNumber(int bound) {
        return ThreadLocalRandom.current().nextInt(0, bound);
    }
}
