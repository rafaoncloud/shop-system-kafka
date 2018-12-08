package com.data;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;

public class BalanceTransactions {

    private static BalanceTransactions single_instance = null;

    private static SessionFactory factory;

    private BalanceTransactions() {
        try {
            factory = new Configuration().configure().buildSessionFactory();
        } catch (Throwable ex) {
            System.err.println("Failed to create sessionFactory object." + ex);
            throw new ExceptionInInitializerError(ex);
        }
    }

    public static BalanceTransactions getInstance() {
        if (single_instance == null)
            single_instance = new BalanceTransactions();

        return single_instance;
    }

    public Integer addBalance() {
        Session session = factory.openSession();
        Transaction tx = null;
        Integer balanceID = null;

        try {
            tx = session.beginTransaction();
            Balance balance = new Balance(null, 0);
            balanceID = (Integer) session.save(balance);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        return balanceID;
    }

    public Balance getBalance() throws Exception {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Balance balance = (Balance) session.get(Balance.class, 1);
            return balance;
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        throw new Exception("Item Balance not found.");
    }

    public void updateBalance(int balanceValue) {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Balance balance = (Balance) session.get(Balance.class, 1);
            balance.setBalance(balanceValue);
            session.update(balance);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void balanceIncrease(int moneyToIncrease) {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Balance balance = (Balance) session.get(Balance.class, 1);
            balance.setBalance(balance.getBalance() + moneyToIncrease);
            session.update(balance);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void balanceDeduce(int moneyToDeduce) {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Balance balance = (Balance) session.get(Balance.class, 1);
            balance.setBalance(balance.getBalance() - moneyToDeduce);
            session.update(balance);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void dropTable() {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            session.createSQLQuery("DROP TABLE Balance").executeUpdate();
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

}
