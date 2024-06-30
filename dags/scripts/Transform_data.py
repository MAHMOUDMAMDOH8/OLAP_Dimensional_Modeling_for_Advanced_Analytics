import pandas as pd
import logging
import os
from scripts.Tables_creations import *

def Get_Customerdim(host,db_name,user,password):
    logging.info('start Extracting dim customers')
    try:
        conn = create_connection(host,db_name,user,password)
        if conn:
            cursor = conn.cursor()

            cursor.execute("select * from customers")
            Customer_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

            logging.info("Extract dim customer done")
            close_connection(conn)
            return Customer_df
        
    except Exception as E:
        logging.info(f'Error while Extract customer dimension: {str(E)}')
        return None
    


def Get_productdim(host, db_name, user, password):
    logging.info('Start Extracting dim product')
    try:
        conn = create_connection(host, db_name, user, password)
        if conn:
            cursor = conn.cursor()

            cursor.execute("""SELECT p.id , p.product_code, p.product_name, 
                                     p.description, p.standard_cost, p.list_price,
                                     p.reorder_level, p.target_level, p.quantity_per_unit,
                                     p.discontinued, p.minimum_reorder_quantity, p.category,
                                     p.attachments 
                              FROM products p""")
            products_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

            logging.info("Extract dim products done")
            close_connection(conn)
            return products_df
        
    except Exception as E:
        logging.error(f'Error while Extracting products dimension: {str(E)}')
        return None


def Get_Employeedim(host,db_name,user,password):
    logging.info('start Extracting dim employee')
    try:
        conn = create_connection(host,db_name,user,password)
        if conn:
            cursor = conn.cursor()

            cursor.execute("select  * from employees ")
            employees_df = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

            logging.info("Extract dim employees done")
            close_connection(conn)
            return employees_df
        
    except Exception as E:
        logging.info(f'Error while Extract employees dimension: {str(E)}')
        return None
    

def Get_factSales(host, db_name, user, password):
    logging.info('start Extracting fact sales')
    try:
        conn = create_connection(host, db_name, user, password)
        if conn:
            cursor = conn.cursor()
            cursor.execute("""SELECT o.id AS "order_id", product_id, employee_id, customer_id, 
                                     o.order_date, o.payment_type, 
                                     od.quantity, od.unit_price, od.discount, od.status_id  
                              FROM orders o 
                              JOIN order_details od ON o.id = od.order_id""")
            Fact_Salesdf = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
            logging.info("Extract fact sales done")
            close_connection(conn)
            return Fact_Salesdf
        
    except Exception as E:
        logging.info(f'Error while Extract Fact_Sales: {str(E)}')
        return None