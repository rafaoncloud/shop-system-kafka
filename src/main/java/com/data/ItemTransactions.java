package com.data;


import jdk.jshell.spi.ExecutionControl;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;

import java.util.List;

/**
 * Singleton
 * CRUD Transactions
 */
public class ItemTransactions {

    private static ItemTransactions single_instance = null;

    private static SessionFactory factory;

    private ItemTransactions() {
        try {
            factory = new Configuration().configure().buildSessionFactory();
        } catch (Throwable ex) {
            System.err.println("Failed to create sessionFactory object." + ex);
            throw new ExceptionInInitializerError(ex);
        }
    }

    public static ItemTransactions getInstance() {
        if (single_instance == null)
            single_instance = new ItemTransactions();

        return single_instance;
    }

    public Integer addItem(String name, int price, int amount) {
        Session session = factory.openSession();
        Transaction tx = null;
        Integer itemID = null;

        try {
            tx = session.beginTransaction();
            Item item = new Item(null, name, price, amount);
            itemID = (Integer) session.save(item);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        return itemID;
    }

    public Item getItem(Long itemID) throws Exception {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Item item = (Item) session.get(Item.class, itemID);
            tx.commit();
            return item;
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        throw new Exception("Item " + itemID + " not found.");
    }

    public Item getItem(String name) throws Exception {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Query query = session.createQuery("FROM Item i WHERE i.name = :name");
            query.setString("name", name);
            Item item = (Item) query.list().get(0);
            tx.commit();
            return item;
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        throw new Exception("[Shop] Item " + name + " not found.");
    }

    // Debug
    public List<Item> listItems() {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            List<Item> items = session.createQuery("FROM Item").list();
//            for (Iterator iterator = itens.iterator(); iterator.hasNext(); ) {
//                Item item = (Item) iterator.next();
//                System.out.print("Item ID: " + item.getItemID());
//                System.out.print("Name: " + item.getName());
//                System.out.println("Price: " + item.getPrice());
//            }
            //tx.commit();
            return items;
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        return null;
    }

    public void increaseItemAmount(String name, int amountToIncrease) throws Exception {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            //Item item = (Item) session.get(Item.class, itemID);
            Item item = getItem(name);
            item.setAmount(item.getAmount() + amountToIncrease);
            session.update(item);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void decreaseItemAmount(String name, int amountToDeduce) throws Exception {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            //Item item = (Item) session.get(Item.class, itemID);
            Item item = getItem(name);
            item.setAmount(item.getAmount() - amountToDeduce);
            session.update(item);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void updateItem(Integer itemID, String name, int price, int amount) {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Item item = (Item) session.get(Item.class, itemID);
            item.setName(name);
            item.setPrice(price);
            item.setAmount(amount);
            session.update(item);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void deleteItem(Integer EmployeeID) {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Item item = (Item) session.get(Item.class, EmployeeID);
            session.delete(item);
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void dropAndCreateTable() {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            session.createSQLQuery("DROP TABLE item").executeUpdate();
            tx.commit();
            tx = session.beginTransaction();
            session.createSQLQuery("CREATE TABLE item " +
                    "(id INTEGER UNSIGNED AUTO_INCREMENT PRIMARY KEY," +
                    " name VARCHAR(50) NOT NULL," +
                    " price INTEGER NOT NULL," +
                    " amount INTEGER NOT NULL);").executeUpdate();
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            //e.printStackTrace();
        } finally {
            session.close();
        }
    }
}
