DIM_Customers = """
CREATE TABLE IF NOT EXISTS DIM_Customers(
        customer_id int,
        company VARCHAR,
        last_name VARCHAR,
        first_name VARCHAR,
        email_address VARCHAR,
        job_title VARCHAR,
        business_phone VARCHAR,
        home_phone VARCHAR,
        mobile_phone VARCHAR,
        fax_number VARCHAR,
        address VARCHAR,
        city VARCHAR,
        state_province VARCHAR,
        zip_postal_code VARCHAR,
        country_region VARCHAR,
        web_page VARCHAR,
        notes VARCHAR,
        attachments VARCHAR,
        start_date DATE,
        end_date DATE,
        active_flag CHAR(1),
        version INT,
        PRIMARY KEY (customer_id, version)
)
"""

DIM_Employee = """
CREATE TABLE IF NOT EXISTS DIM_Employee(
        Employee_id int,
        company VARCHAR,
        last_name VARCHAR,
        first_name VARCHAR,
        email_address VARCHAR,
        job_title VARCHAR,
        business_phone VARCHAR,
        home_phone VARCHAR,
        mobile_phone VARCHAR,
        fax_number VARCHAR,
        address VARCHAR,
        city VARCHAR,
        state_province VARCHAR,
        zip_postal_code VARCHAR,
        country_region VARCHAR,
        web_page VARCHAR,
        notes VARCHAR,
        attachments VARCHAR,
        start_date DATE,
        end_date DATE,
        active_flag CHAR(1),
        version INT,
        PRIMARY KEY (Employee_id , version)
)
"""

DIM_Products = """
CREATE TABLE IF NOT EXISTS DIM_Products(
        Product_id SERIAL PRIMARY KEY,
        product_code VARCHAR,
        product_name VARCHAR,
        description VARCHAR,
        standard_cost NUMERIC,
        list_price NUMERIC,
        reorder_level INT,
        target_level INT,
        quantity_per_unit VARCHAR,
        discontinued INT,
        minimum_reorder_quantity INT,
        category VARCHAR,
        attachments VARCHAR
)
"""

Fact_Sales = """
CREATE TABLE IF NOT EXISTS Fact_Sales(
        order_id int,
        Product_id int,
        Employee_id int,
        customer_id int,
        order_date TIMESTAMP,
        payment_type VARCHAR,
        Quantity int,
        unit_price float,
        discount float,
        status_id int
)
"""
