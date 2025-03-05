import mysql.connector
import utils

url = utils.url
host = utils.host
user = utils.user
password = utils.password
database = utils.database

db_config = {
    "host": host,
    "user":user,
    "password": password,
    "database": database}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()


# ************************************CREATING TEMP TABLES*******************

create_temp_customer = """
CREATE TABLE IF NOT EXISTS temp_customer(
    CustomerID VARCHAR(255),
    FirstName VARCHAR(255),
    LastName VARCHAR(255),
    Age BIGINT,
    Gender VARCHAR(255),
    Email VARCHAR(255),
    Phone VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(255),
    PostalCode VARCHAR(255),
    CustomerIncome VARCHAR(255),
    CustomerSince VARCHAR(255),
    CustomerStatus VARCHAR(255),
    CustomerChurn VARCHAR(255)
);

"""

cursor.execute(create_temp_customer)
conn.commit()
print("temp_Customer Table created sucessfully")

create_temp_Agent = """
CREATE TABLE IF NOT EXISTS temp_agent (
    agent_id VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    mobile_no VARCHAR(255),
    email VARCHAR(255),
    city VARCHAR(255),
    gender VARCHAR(255),
    age BIGINT,
    hire_date VARCHAR(255),
    department VARCHAR(255),
    job_title VARCHAR(255),
    status VARCHAR(255)
);
"""
cursor.execute(create_temp_Agent)
conn.commit()
print("temp_Agent Table created sucessfully")

create_temp_Org = """
CREATE TABLE IF NOT EXISTS temp_organization (
    OrganizationID VARCHAR(255),
    OrgName VARCHAR(255),
    Industry VARCHAR(255),
    ContactPerson VARCHAR(255),
    ContactPhone VARCHAR(255),
    ContactEmail VARCHAR(255),
    Address VARCHAR(255),
    PostalCode BIGINT,
    Country VARCHAR(255),
    City VARCHAR(255)
);
"""
cursor.execute(create_temp_Org)
conn.commit()
print("temp_org Table created sucessfully")


create_temp_Product = """
CREATE TABLE IF NOT EXISTS temp_product(
    ProductID VARCHAR(255),
    ProductName VARCHAR(255),
    Category VARCHAR(255),
    Description VARCHAR(255),
    Manufacturer VARCHAR(255),
    Price BIGINT,
    Dimensions VARCHAR(255),
    SKU VARCHAR(255),
    Weight VARCHAR(255)
);
"""
cursor.execute(create_temp_Product)
conn.commit()
print("temp_product Table created sucessfully")


# ************************************CREATING MAIN TABLES*******************


create_main_customer = """
CREATE TABLE IF NOT EXISTS main_customer(
    cust_key_id INT AUTO_INCREMENT PRIMARY KEY,
    CustomerID VARCHAR(255),
    FirstName VARCHAR(255),
    LastName VARCHAR(255),
    Age BIGINT,
    Gender VARCHAR(255),
    Email VARCHAR(255),
    Phone VARCHAR(255),
    Address VARCHAR(255),
    City VARCHAR(255),
    State VARCHAR(255),
    Country VARCHAR(255),
    PostalCode VARCHAR(255),
    CustomerIncome VARCHAR(255),
    CustomerSince VARCHAR(255),
    CustomerStatus VARCHAR(255),
    CustomerChurn VARCHAR(255),
    active_flag INT,
    start_date DATE,
    end_date DATE 
);

"""
cursor.execute(create_main_customer)
conn.commit()
print("main_Customer Table created sucessfully")

create_main_Agent = """
CREATE TABLE IF NOT EXISTS main_agent (
    agent_key_id INT AUTO_INCREMENT PRIMARY KEY,
    agent_id VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    mobile_no VARCHAR(255),
    email VARCHAR(255),
    city VARCHAR(255),
    gender VARCHAR(255),
    age BIGINT,
    hire_date VARCHAR(255),
    department VARCHAR(255),
    job_title VARCHAR(255),
    status VARCHAR(255),
    active_flag INT,
    start_date DATE,
    end_date DATE 
);
"""

cursor.execute(create_main_Agent)
conn.commit()
print("main_Agent Table created sucessfully")


create_main_Org = """
CREATE TABLE IF NOT EXISTS main_organization (
    org_key_id INT AUTO_INCREMENT PRIMARY KEY,
    OrganizationID VARCHAR(255),
    OrgName VARCHAR(255),
    Industry VARCHAR(255),
    ContactPerson VARCHAR(255),
    ContactPhone VARCHAR(255),
    ContactEmail VARCHAR(255),
    Address VARCHAR(255),
    PostalCode BIGINT,
    Country VARCHAR(255),
    City VARCHAR(255),
    active_flag INT,
    start_date DATE,
    end_date DATE
);
"""
cursor.execute(create_main_Org)
conn.commit()
print("main_org Table created sucessfully")


create_main_Product = """
CREATE TABLE IF NOT EXISTS main_product (
    prd_key_id INT AUTO_INCREMENT PRIMARY KEY,
    ProductID VARCHAR(255),
    ProductName VARCHAR(255),
    Category VARCHAR(255),
    Description VARCHAR(255),
    Manufacturer VARCHAR(255),
    Price BIGINT,
    Dimensions VARCHAR(255),
    SKU VARCHAR(255),
    Weight VARCHAR(255),
    active_flag INT,
    start_date DATE,
    end_date DATE
);
"""
cursor.execute(create_main_Product)
conn.commit()
print("main_product Table created sucessfully")


create_main_Date = """
CREATE TABLE IF NOT EXISTS main_date  (
    DateID  VARCHAR(255) PRIMARY KEY,
    full_date DATE,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_of_week INT,
    day_name VARCHAR(9),
    is_weekend BOOLEAN
);
"""
cursor.execute(create_main_Date)
conn.commit()
print("main_date Table created sucessfully")

insert_date_data="""
INSERT INTO main_date (DateID, full_date, year, quarter, month, day, day_of_week, day_name, is_weekend)
SELECT
    DATE_FORMAT(date, '%Y%m%d') AS DateID,
    date AS full_date,
    YEAR(date) AS year,
    QUARTER(date) AS quarter,
    MONTH(date) AS month,
    DAY(date) AS day,
    DAYOFWEEK(date) AS day_of_week,
    UPPER(DAYNAME(date)) AS day_name,
    DAYOFWEEK(date) IN (1, 7) AS is_weekend
FROM
    (
        SELECT
            DATE_ADD('2023-07-01', INTERVAL (t4*1000 + t3*100 + t2*10 + t1) DAY) AS date
        FROM
            (SELECT 0 AS t1 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) AS num1
            CROSS JOIN (SELECT 0 AS t2 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) AS num2
            CROSS JOIN (SELECT 0 AS t3 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) AS num3
            CROSS JOIN (SELECT 0 AS t4 UNION SELECT 1 UNION SELECT 2) AS num4
    ) AS date_generator
WHERE
    date BETWEEN '2023-12-23' AND  DATE_FORMAT(CURDATE(), '%Y%m%d');

"""
cursor.execute(insert_date_data)
conn.commit()
print("INSERTED DATA TO DATE Table created sucessfully")


create_main_Integrated = """
CREATE TABLE IF NOT EXISTS main_integrated (
    interaction_id VARCHAR(255) PRIMARY KEY,
    customer_id int,
    agent_id int,
    organisation_id int,
    product_id int,
    created_date_id VARCHAR(255) NOT NULL,
    department VARCHAR(255),
    open_date TIMESTAMP,
    close_date TIMESTAMP,
    interaction_date DATE,
    interaction_time TEXT NOT NULL,
    interaction_type VARCHAR(255),
    response_time VARCHAR(255),
    interaction_channel VARCHAR(255),
    origin VARCHAR(255),
    purchase_amount BIGINT,
    feedback_rating BIGINT,
    status VARCHAR(255),
    transcript TEXT NOT NULL,
    intent VARCHAR(255),
    intent_score VARCHAR(255),
    csat_score BIGINT,
    nps_score BIGINT,
    language VARCHAR(255),
    customer_positive_keywords VARCHAR(255),
    customer_negative_keywords VARCHAR(255),
    agent_positive_keywords VARCHAR(255),
    agent_negative_keywords VARCHAR(255),
    agent_sentiment VARCHAR(255),
    agent_sentiment_score VARCHAR(255),
    customer_sentiment VARCHAR(255),
    customer_sentiment_score VARCHAR(255),
    FOREIGN KEY (customer_id) REFERENCES main_customer(cust_key_id),
    FOREIGN KEY (agent_id) REFERENCES main_agent(agent_key_id),
    FOREIGN KEY (organisation_id) REFERENCES main_organization(org_key_id),
    FOREIGN KEY (product_id) REFERENCES main_product(prd_key_id),
    FOREIGN KEY (created_date_id) REFERENCES main_date(DateID)
);
"""

cursor.execute(create_main_Integrated)
conn.commit()
print("main_Integrated Table created sucessfully")



# ************************************CREATING STORED PROCEDURES*******************

#----------Agent 
drop_procedure_agent = "DROP PROCEDURE IF EXISTS merge_agent_data;"
cursor.execute(drop_procedure_agent)
conn.commit()

create_procedure_agent = """
CREATE PROCEDURE merge_agent_data()
BEGIN
    -- Update existing records
    UPDATE main_agent AS target
    JOIN temp_agent AS source ON source.agent_id = target.agent_id
    SET target.active_flag = 0, target.end_date = CURDATE()
    WHERE target.active_flag = 1;

    -- Insert new records
    INSERT INTO main_agent (agent_id, first_name, last_name, mobile_no, email, city, gender, age, hire_date, department, job_title, status, active_flag, start_date, end_date)
    SELECT agent_id, first_name, last_name, mobile_no, email, city, gender, age, hire_date, department, job_title, status, 1, CURDATE(), '9999-12-31'
    FROM temp_agent;

    -- Clear temporary table
    TRUNCATE TABLE temp_agent;
END;
"""

# Execute the SQL code to create the stored procedure
cursor.execute(create_procedure_agent)
conn.commit()
print("agent stored procedure created successfully")

#------------------>CUSTOMER

drop_procedure_customer = "DROP PROCEDURE IF EXISTS merge_cust_data;"
cursor.execute(drop_procedure_customer)
conn.commit()

create_procedure_customer = """
CREATE PROCEDURE merge_cust_data()
BEGIN 
    UPDATE main_customer AS target
    JOIN temp_customer AS source ON source.CustomerID = target.CustomerID
    SET target.active_flag = 0 , target.end_date = CURDATE()
    WHERE target.active_flag = 1;
    
    INSERT INTO main_customer (CustomerID,FirstName,LastName,Age,Gender,Email,Phone,Address,City,State,Country,PostalCode,CustomerIncome,CustomerSince,CustomerStatus,CustomerChurn,active_flag,start_date,end_date)
    SELECT CustomerID,FirstName,LastName,Age,Gender,Email,Phone,Address,City,State,Country,PostalCode,CustomerIncome,CustomerSince,CustomerStatus,CustomerChurn,1,CURDATE(),'9999-12-31'
    FROM temp_customer;
    
    TRUNCATE TABLE temp_customer;
END;
"""

# Execute the SQL code to create the stored procedure
cursor.execute(create_procedure_customer)
conn.commit()
print("cust stored procedure created successfully")

#---------------------------->ORGANIZATION

drop_procedure_org = "DROP PROCEDURE IF EXISTS merge_organization_data;"
cursor.execute(drop_procedure_org)
conn.commit()

create_procedure_org = """
CREATE PROCEDURE merge_organization_data()
BEGIN
    -- Update existing records
    UPDATE main_organization AS target
    JOIN temp_organization AS source ON source.OrganizationID = target.OrganizationID
    SET target.active_flag = 0, target.end_date = CURDATE()
    WHERE target.active_flag = 1;

    -- Insert new records
    INSERT INTO main_organization (OrganizationID, OrgName, Industry, ContactPerson, ContactPhone, ContactEmail, Address, PostalCode, Country, City, active_flag, start_date, end_date)
    SELECT OrganizationID, OrgName, Industry, ContactPerson, ContactPhone, ContactEmail, Address, PostalCode, Country, City, 1, CURDATE(), '9999-12-31'
    FROM temp_organization;

    -- Clear temporary table
    TRUNCATE TABLE temp_organization;
END;
"""

# Execute the SQL code to create the stored procedure
cursor.execute(create_procedure_org)
conn.commit()
print("organization stored procedure created successfully")

#----------------------->PRODUCT


drop_procedure_product = "DROP PROCEDURE IF EXISTS merge_product_data;"
cursor.execute(drop_procedure_product)
conn.commit()

create_procedure_product = """
CREATE PROCEDURE merge_product_data()
BEGIN
    -- Update existing records
    UPDATE main_product AS target
    JOIN temp_product AS source ON source.ProductID = target.ProductID
    SET target.active_flag = 0, target.end_date = CURDATE()
    WHERE target.active_flag = 1;

    -- Insert new records
    INSERT INTO main_product (ProductID, ProductName, Category, Description, Manufacturer, Price, Dimensions, SKU, Weight, active_flag, start_date, end_date)
    SELECT ProductID, ProductName, Category, Description, Manufacturer, Price, Dimensions, SKU, Weight, 1, CURDATE(), '9999-12-31'
    FROM temp_product;

    -- Clear temporary table
    TRUNCATE TABLE temp_product;
END; 
"""

# Execute the SQL code to create the stored procedure
cursor.execute(create_procedure_product)
conn.commit()
print("product stored procedure created successfully")

#------------------------->INTEGRATED


drop_procedure_integrated = "DROP PROCEDURE IF EXISTS UpdateOrInsertmain_integrated;"
cursor.execute(drop_procedure_integrated)
conn.commit()

cursor.close()
conn.close()