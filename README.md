# Northwind Database OLTP to OLAP Transformation: Leveraging Dimensional Modeling for Advanced Analytics

## Table of Contents
- [Identifying Business Requirements](#Identifying-Business-Requirements)
- [Database Schema](#database-schema)
- [Tech Stack & Tools](#tech-stack--tools)

# Aim

To modernize the data reporting solution for Northwind through Dimensional Modeling.
Identifying Business Requirements

# Identifying Business Requirements

There are many business processes that can be derived from the Northwind database through the E-R diagram. However, we will be focusing on one process:

    Sales Overview: Overall sales reports to better understand what is being sold to our customers, what sells the most, where, and what sells the least. The goal is to have a general overview of how the business is performing.
    
Here is the ERD for Northwind OLTP:

![northwind-oltp-erd](https://github.com/MAHMOUDMAMDOH8/OLAP_Dimensional_Modeling_for_Advanced_Analytics/assets/111503676/3e6d12ef-fa3a-4f03-82fb-c867b65bc343)


Customers - Customers who buy items from Northwind
Employees - Those who work for Northwind
Orders - Sales Order transactions taking place between the customers & Northwind
Order Details - Order Details for the Orders placed by customer
Inventory Transaction - Transaction details of each inventory
Products - Current Northwind products that customers can purchase
Shippers - Shipped orders from Northwind to customers
Suppliers - Supplies Northwind with required items
Invoices - Invoice created for each order



### Assumptions
- The nothwind data is assumed to be clean and does not require extensive cleaning or preprocessing.
- The database schema follows a star schema design for efficient querying and analysis.
### Step 1: Data Collection and Storage
We have  one Data Sources:
  - northind  Data: Stored in postgres . 
transform_data.py:
 -  Data is initially stored in a database.
 - Get_Customerdim()
 - - Selects columns needed from the Customers table.
 - Get_productdim()
 - - Selects columns needed from the Products table.
 - Get_Employeedim()
 - - Selects columns needed from the Employees table.
 - Get_factSales()
 -  - Selects columns needed from the Orders and Order Details tables.
### Step 2: Data Delivery
 - tables_creation.py:
    - Contains functions to create, and load tables, reading the statements from sql_queries.py for preparing the data warehouse.
 - load_dimCustomers():
        - load transformed customer data from  database, selecting required columns and loaded into a PostgreSQL database 
        - Load transformed sales data from silver layer, selecting required columns and loaded into a PostgreSQL database in the Gold layer and apply scd type 2 
 - load_DIM_Employee() :
        -  load transformed  employee data from  database, selecting required columns and loaded into a PostgreSQL database 
        -  Load transformed sales data from silver layer, selecting required columns and loaded into a PostgreSQL database in the Gold layer and apply scd type 2 
 - load_DIM_Products():
        -  Extracts and cleans rate data from the database ,and loaded into a PostgreSQL database in the Gold layerles_dim() and apply scd type 1 
 - load_fact():
        - load transformed customer data from  silver layer, selecting required columns and loaded into a PostgreSQL database 
   
  db_utils.py
    - The `db_utils.py` module provides a set of database utility functions for interacting with the PostgreSQL database. 
    - These functions handle database connections, data loading, and query execution, ensuring efficient and reliable data management within the pipeline.



# Database Schema

dim_customers: Contains information about customers.

    Columns: customer_id, company, last_name, first_name, email_address, job_title, business_phone, home_phone, mobile_phone, fax_number, address, city, state_province, zip_postal_code, country_region, web_page, notes, attachments, start_date, end_date, active_flag, version.

dim_employee: Contains information about employees.

    Columns: employee_id, company, last_name, first_name, email_address, job_title, business_phone, home_phone, mobile_phone, fax_number, address, city, state_province, zip_postal_code, country_region, web_page, notes, attachments, start_date, end_date, active_flag, version.
    
dim_product: Contains information about products.

    Columns: product_id, product_code, product_name, description, standard_cost, list_price, reorder_level, target_level, quantity_per_unit, discontinued, minimum_reorder_quantity, category, attachments.

fact_sales: Contains sales data.

    Columns: order_id, product_id, employee_id, customer_id, order_date, payment_type, quantity, unit_price, discount, status_id.


![Flowcharts](https://github.com/MAHMOUDMAMDOH8/OLAP_Dimensional_Modeling_for_Advanced_Analytics/assets/111503676/04b45fb2-fd07-4ec2-94cb-43506e1944a1)


# Tech Stack & Tools

Infrastructure: Docker

Data Warehouse: PostgreSQL

Database: PostgreSQL

Orchestration: Apache Airflow

ETL Scripts:   Python



