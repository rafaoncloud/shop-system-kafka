package com;

import com.data.ItemTransactions;

public class Main {

    public static final ItemTransactions itemTransaction = ItemTransactions.getInstance();

    public static void main(String[] args) {
        populateDatabaseWithItens();


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
}
