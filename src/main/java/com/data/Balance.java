package com.data;

public class Balance {

    protected Integer balanceID;
    protected int balance;

    public Balance() {
    }

    public Balance(Integer balanceID, int balance) {
        this.balanceID = balanceID;
        this.balance = balance;
    }

    public Integer getBalanceID() {
        return balanceID;
    }

    public void setBalanceID(Integer balanceID) {
        this.balanceID = balanceID;
    }

    public int getBalance() {
        return balance;
    }

    public void setBalance(int balance) {
        this.balance = balance;
    }
}
