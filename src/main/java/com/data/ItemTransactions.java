package com.data;


import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

/**
 * Singleton
 * CRUD Transactions
 */
public class ItemTransactions {

    private static ItemTransactions single_instance = null;

    private static SessionFactory factory;

    public ItemTransactions() {
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

    public Integer addItem(String name, int price) {
        Session session = factory.openSession();
        Transaction tx = null;
        Integer itemID = null;

        try {
            tx = session.beginTransaction();
            Item item = new Item(null,name, price);
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
            return item;
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
        throw new Exception("Item " + itemID + " not found.");
    }

    // Debug
    public void listItems() {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            List<Item> itens = session.createQuery("FROM Item").list();
//            for (Iterator iterator = itens.iterator(); iterator.hasNext(); ) {
//                Item item = (Item) iterator.next();
//                System.out.print("Item ID: " + item.getItemID());
//                System.out.print("Name: " + item.getName());
//                System.out.println("Price: " + item.getPrice());
//            }
            tx.commit();
        } catch (HibernateException e) {
            if (tx != null) tx.rollback();
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void updateItem(Item item) {
        throw new NotImplementedException();
    }

    public void updateItem(Integer EmployeeID, String name, int price) {
        Session session = factory.openSession();
        Transaction tx = null;

        try {
            tx = session.beginTransaction();
            Item item = (Item) session.get(Item.class, EmployeeID);
            item.setName(name);
            item.setPrice(price);
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
}
