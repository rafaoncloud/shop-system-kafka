package com.data;


public class Item {

    protected Integer ItemID;
    protected String name;
    protected int price;
    protected int amount;

    public Item() {
    }

    public Item(Integer itemId, String name, int price, int quantity) {
        ItemID = itemId;
        this.name = name;
        this.price = price;
        this.amount = amount;
    }

    public Integer getItemID() {
        return ItemID;
    }

    public void setItemID(Integer itemId) {
        ItemID = itemId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }
}
