from scripts.Tables_creations import *
from scripts.Transform_data import *
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig(filename='data_delivery.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# load dimCustomers 
def load_dimCustomers(host,db_name,user,password):
    try:
        logging.info('loading dimCustomers to db ...')
        Customer_oltp = Get_Customerdim(host,"northwind_oltp",user,password)
        Customer_oltp.rename(columns={'id': 'customer_id'},inplace=True)

        conn = create_connection(host,db_name,user,password)
        if not conn:
            raise Exception("Connection to database failed")
        
        cursor = conn.cursor()
        
        cursor.execute("select * from northwind_dwh.public.dim_customers")

        Customer_DWH = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

        new_records = []

        current_date = datetime.now().date()

        logging.info("SCD type 2 on dimcustomer started")
        for i , row in Customer_oltp.iterrows():

            
            existing_records = Customer_DWH[Customer_DWH['customer_id'] == row['customer_id']]

            if not existing_records.empty:
                for idx,existing_record in existing_records.iterrows():

                    #check any change 
                    if (existing_record['email_address'] != row['email_address'] or
                        existing_record['job_title'] != row['job_title'] or 
                        existing_record['business_phone'] != row['business_phone'] or
                        existing_record['fax_number'] != row['fax_number'] or
                        existing_record['address'] != row['address'] or
                        existing_record['city'] != row['city'] or
                        existing_record['state_province'] != row['state_province'] or                   
                        existing_record['zip_postal_code'] != row['zip_postal_code']):

                        # end active record
                        if existing_record['active_flag'] == 'Y':
                            query = """
                                 UPDATE dim_customers
                                 SET end_date = %s, active_flag = 'N'
                                 WHERE customer_id = %s AND active_flag = 'Y'
                            """
                            params = (current_date, row['customer_id'])
                            execute_query(conn,query,params,"dim_customers")

                            # New version of record
                            new_record = row.copy()
                            new_record['start_date'] = current_date
                            new_record['end_date'] = None
                            new_record['active_flag'] = 'Y'
                            new_record['version'] = existing_record['version'] + 1
                            new_records.append(new_record)

            else:
                # New record
                row['start_date'] = current_date
                row['end_date'] = None
                row['active_flag'] = 'Y'
                row['version'] = 1
                new_records.append(row)

        if new_records:
            new_records_df = pd.DataFrame(new_records)
            load_to_postgres("dim_customers", new_records_df, host,db_name,user,password)
        close_connection(conn)

    except Exception as E:
        logging.info(f'Error while loading dimCustomers: {str(E)}')

def load_DIM_Employee(host,db_name,user,password):
    try:
        logging.info('loading dimEmployee to db ...')
        employee_oltp = Get_Employeedim(host,"northwind_oltp",user,password)
        employee_oltp.rename(columns={'id': 'employee_id'},inplace=True)

        conn = create_connection(host,db_name,user,password)
        if not conn:
            raise Exception("Connection to database failed")
        
        cursor = conn.cursor()
        
        cursor.execute("select * from northwind_dwh.public.dim_employee")

        Employee_DWH = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

        new_records = []

        current_date = datetime.now().date()

        logging.info("SCD type 2 on dimemployee started")
        for i , row in employee_oltp.iterrows():


            existing_records = Employee_DWH[Employee_DWH['employee_id'] == row['employee_id']]

            if not existing_records.empty:
                for idx,existing_record in existing_records.iterrows():

                    #check any change 
                    if (existing_record['email_address'] != row['email_address'] or
                        existing_record['job_title'] != row['job_title'] or 
                        existing_record['business_phone'] != row['business_phone'] or
                        existing_record['fax_number'] != row['fax_number'] or
                        existing_record['address'] != row['address'] or
                        existing_record['city'] != row['city'] or
                        existing_record['state_province'] != row['state_province'] or                   
                        existing_record['zip_postal_code'] != row['zip_postal_code']):

                        # end active record
                        if existing_record['active_flag'] == 'Y':
                            query = """
                                 UPDATE dim_employee
                                 SET end_date = %s, active_flag = 'N'
                                 WHERE employee_id = %s AND active_flag = 'Y'
                            """
                            params = (current_date, row['employee_id'])
                            execute_query(conn,query,params,"dim_employee")

                            # New version of record
                            new_record = row.copy()
                            new_record['start_date'] = current_date
                            new_record['end_date'] = None
                            new_record['active_flag'] = 'Y'
                            new_record['version'] = existing_record['version'] + 1
                            new_records.append(new_record)

            else:
                # New record
                row['start_date'] = current_date
                row['end_date'] = None
                row['active_flag'] = 'Y'
                row['version'] = 1
                new_records.append(row)

        if new_records:
            new_records_df = pd.DataFrame(new_records)
            load_to_postgres("dim_employee", new_records_df, host,db_name,user,password)
        close_connection(conn)

    except Exception as E:
        logging.info(f'Error while loading dimEmployee: {str(E)}')


def load_DIM_Products(host, db_name, user, password):
    try:
        logging.info('Loading dimproduct to db...')
        product_oltp = Get_productdim(host, "northwind_oltp", user, password)
        product_oltp.rename(columns={'id':'Product_id'},inplace=True)

        conn = create_connection(host, db_name, user, password)
        if not conn:
            raise Exception("Connection to database failed")
        
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM northwind_dwh.public.dim_products")
        product_DWH = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])

        logging.info("SCD type 1 on dimproduct started")
        new_records = []

        for i, row in product_oltp.iterrows():
            existing_records = product_DWH[product_DWH['product_id'] == row['Product_id']]


            
            if not existing_records.empty:

                if (existing_records['standard_cost'].iloc[0] != row['standard_cost'] or
                    existing_records['list_price'].iloc[0] != row['list_price'] or
                    existing_records['quantity_per_unit'].iloc[0] != row['quantity_per_unit'] or
                    existing_records['minimum_reorder_quantity'].iloc[0] != row['minimum_reorder_quantity']):
                    
                    Query = """
                    UPDATE dim_products SET
                    standard_cost = %s,
                    list_price = %s,
                    quantity_per_unit = %s,
                    minimum_reorder_quantity = %s
                    WHERE product_id = %s
                    """
                    params = (
                        row['standard_cost'], row['list_price'], row['quantity_per_unit'], row['minimum_reorder_quantity'],
                        row['Product_id']
                    )
                    execute_query(conn, Query, params, "dim_products")
            else:
                new_records.append(row)

        if new_records:
            new_records_df = pd.DataFrame(new_records)
            # Handle NaN values before converting to integer types
            new_records_df = new_records_df.fillna({
                'reorder_level': 0,
                'target_level': 0,
                'discontinued': 0,
                'minimum_reorder_quantity': 0
            })
            # Ensure data types are appropriate before loading to the database
            new_records_df = new_records_df.astype({
                'Product_id': 'int32',
                'reorder_level': 'int16',
                'target_level': 'int16',
                'discontinued': 'int8',
                'minimum_reorder_quantity': 'int16'
            })
            
            load_to_postgres("dim_products", new_records_df, host, db_name, user, password)
    
        close_connection(conn)
        logging.info("Loading dim_products completed successfully")

    except Exception as E:
        logging.error(f'Error while loading dimProducts: {str(E)}')


def load_Factsales(host, db_name, user, password):
    try:
        logging.info('Loading factsales to db ...')
        Fact_salesdf = Get_factSales(host, "northwind_oltp", user, password)

        # Convert order_date column to datetime and replace NaT with None
        Fact_salesdf['order_date'] = pd.to_datetime(Fact_salesdf['order_date'], errors='coerce')
        Fact_salesdf['order_date'] = Fact_salesdf['order_date'].apply(lambda x: None if pd.isna(x) else x)

        load_to_postgres("fact_sales", Fact_salesdf, host, db_name, user, password)
        logging.info('Fact_Sales loaded successfully')

    except Exception as E:
        logging.info(f'Error while loading Fact_Sales: {str(E)}')
