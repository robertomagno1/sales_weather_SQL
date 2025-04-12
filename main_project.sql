CREATE DATABASE IF NOT EXISTS sales_data ;

CREATE TABLE IF NOT EXISTS sales_data.orders_raw (
    row_id INT,
    order_id VARCHAR(50),
    order_date VARCHAR(20),  -- <-- string!
    ship_date VARCHAR(20),   -- <-- string!
    ship_mode VARCHAR(50),
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    segment VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(50),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(200),
    sales DECIMAL(10,2),
    quantity INT,
    discount DECIMAL(5,2),
    profit DECIMAL(10,2)
);

select * from sales1.orders_raw;


-- Temperature table with composite primary key
CREATE TABLE temperature (
    date DATE NOT NULL,
    city VARCHAR(50) NOT NULL,
    temperature DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (date, city),  -- Composite primary key
    INDEX idx_city (city),
    INDEX idx_date (date)
);

-- Humidity table with composite primary key
CREATE TABLE humidity (
    date DATE NOT NULL,
    city VARCHAR(50) NOT NULL,  -- Changed from VARCHAR(10) to match temperature table
    humidity DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (date, city),  -- Composite primary key
    INDEX idx_city (city),
    INDEX idx_date (date)
);

CREATE TABLE description (
    date DATE NOT NULL,
    city VARCHAR(50) NOT NULL,  -- Changed from VARCHAR(10) to match temperature table
    description VARCHAR(50),
    PRIMARY KEY (date, city),  -- Composite primary key
    INDEX idx_city (city),
    INDEX idx_date (date)
);



-- ### aggregate all the we
CREATE TABLE combined_weather1
SELECT 
    t.city,
    t.date,
    t.temperature,
    h.humidity,
    d.description
FROM 
    temperature t
LEFT JOIN 
    humidity h ON t.city = h.city AND t.date = h.date
LEFT JOIN 
    description d ON t.city = d.city AND t.date = d.date;


-- view: 
CREATE VIEW sales_data.combined_weather AS
SELECT 
    t.date,
    t.city,
    t.temperature,
    h.humidity,
    d.description
FROM 
    sales_data.temperature t
JOIN 
    sales_data.humidity h ON t.date = h.date AND t.city = h.city
JOIN 
    sales_data.description d ON t.date = d.date AND t.city = h.city;
    

-- Join orders_raw with weather_condition (note: order_date is a string in orders_raw)
-- Correctly parse 'DD/MM/YY' dates in orders_raw


-- Correctly parse 'DD/MM/YY' dates in orders_raw
CREATE TABLE sales_weather AS 
SELECT 
  o.*, 
  w.temperature, 
  w.humidity, 
  w.description
FROM 
  sales_data.orders_raw o
INNER JOIN sales_data.weather_condition w 
  ON STR_TO_DATE(o.order_date, '%d/%m/%y') = w.date  -- Fixed format!
  AND o.city = w.city;

-- ok 

-- query on sales_weather ; 
-- 1. Temperature and humidity distribution:
SELECT City, AVG(temperature) AS avg_temp, AVG(humidity) AS avg_humidity
FROM sales_weather
GROUP BY City;

-- 2. Average sales by region and city:
SELECT Region, City, AVG(Sales) AS avg_sales
FROM sales_weather
GROUP BY Region, City; 

-- 3. Analysis of sales performance by region and state:
SELECT Region, State, SUM(Sales) AS Total_Sales, SUM(Profit) AS Total_Profit
FROM sales_weather
GROUP BY Region, State
ORDER BY Total_Sales DESC;

-- 4. Identification of the most profitable customers:
SELECT Customer_ID, Customer_Name, SUM(Profit) AS Total_Profit
FROM sales_weather
GROUP BY Customer_ID, Customer_Name
ORDER BY Total_Profit DESC;

-- 5. Identification of the most profitable customers (variant with total sales):
SELECT Customer_ID, Customer_Name, 
       SUM(Sales) AS Total_Sales, 
       SUM(Profit) AS Total_Profit
FROM sales_weather
GROUP BY Customer_ID, Customer_Name
ORDER BY Total_Profit DESC;

-- 6. Product performance by segment:
SELECT Segment, Category, Sub_Category, 
       COUNT(Order_ID) AS Number_of_Orders, 
       SUM(Sales) AS Total_Sales, 
       SUM(Profit) AS Total_Profit
FROM sales_weather
GROUP BY Segment, Category, Sub_Category
ORDER BY Segment, Total_Sales DESC;


-- Optimized Version with Permanent Date Conversion

--  Add a proper DATE column to orders_raw
ALTER TABLE sales_data.orders_raw 
ADD COLUMN order_date_proper DATE;

-- Populate it by converting the string dates
UPDATE sales_data.orders_raw 
SET order_date_proper = STR_TO_DATE(order_date, '%d/%m/%y');


ALTER TABLE orders_raw 
ADD COLUMN ship_date_proper DATE;


ALTER TABLE orders_raw
DROP COLUMN shiop_date_proper;


-- Populate it by converting the string dates
UPDATE orders_raw 
SET ship_date_proper = STR_TO_DATE(order_date, '%d/%m/%y');




select * from sales_data.orders_raw ;


CREATE OR REPLACE VIEW sales_data.orders_raw_view AS
SELECT 
    row_id,
    order_id,
    order_date_proper,
    ship_date_proper,
    ship_mode,
    customer_id,
    customer_name,
    segment,
    country,
    city,
    state,
    postal_code,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit
FROM sales_data.orders_raw;

--  optimized verison of the table sales_weather , adding integrity contstrainght , primary key and indexing ..


CREATE TABLE IF NOT EXISTS sales_data.sales_weather_clean (
    row_id VARCHAR(50) NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    order_date DATE NOT NULL,
    ship_date DATE,
    ship_mode VARCHAR(50),
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    segment VARCHAR(50),
    country VARCHAR(50) NOT NULL,
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50),
    postal_code VARCHAR(20),
    region VARCHAR(50),
    product_id VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(200),
    sales DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    discount DECIMAL(5,2) CHECK (discount >= 0 AND discount <= 1),
    profit DECIMAL(10,2),
    
    -- Weather fields
    temperature DECIMAL(10,2),
    humidity DECIMAL(10,2),
    weather_description VARCHAR(50),

    -- Indexes (senza UNIQUE su order_id)
    INDEX idx_order_id (order_id),
    INDEX idx_customer (customer_id),
    INDEX idx_product (product_id),
    INDEX idx_city (city),
    INDEX idx_order_date (order_date),
    INDEX idx_state (state),
    INDEX idx_region (region)
);

INSERT INTO sales_data.sales_weather_clean (
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    customer_name,
    segment,
    country,
    city,
    state,
    postal_code,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit,
    temperature,
    humidity,
    weather_description
)
SELECT 
    row_id,
    order_id,
    STR_TO_DATE(order_date, '%d/%m/%y'),
    STR_TO_DATE(ship_date, '%d/%m/%y'),
    ship_mode,
    customer_id,
    customer_name,
    segment,
    country,
    city,
    state,
    postal_code,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit,
    temperature,
    humidity,
    description AS weather_description
FROM sales_data.sales_weather;


## this made to check why order_id cant be unique ... customers buy more product with one singol order ....
SELECT order_id, COUNT(*) as cnt
FROM sales_data.sales_weather
GROUP BY order_id
HAVING cnt > 1
ORDER BY cnt DESC;



-- Basic sub-cetegory product performance by Weather Type ::  NEASTED QUERY ..
SELECT 
    w.description AS weather_type,
    o.sub_category,
    COUNT(*) AS order_count,
    SUM(o.quantity) AS total_units,
    ROUND(SUM(o.sales), 2) AS total_sales
FROM 
    sales_data.orders_raw_view o
JOIN (
    SELECT date, city, description 
    FROM sales_data.weather_condition
) w ON o.order_date_proper = w.date AND o.city = w.city
GROUP BY 
    w.description, 
    o.sub_category
ORDER BY 
    total_sales DESC
LIMIT 10;



-- cetegory product performance by Weather Type ::  NEASTED QUERY ..

SELECT 
    w.description,
    o.category,
    COUNT(*) AS orders,
    SUM(o.quantity) AS items_sold
FROM 
    sales_data.orders_raw_view o,
    (SELECT date, city, description FROM sales_data.weather_condition) w
WHERE 
    o.order_date_proper = w.date 
    AND o.city = w.city
GROUP BY 
    w.description, o.category
ORDER BY 
    items_sold DESC;
    

-- 	Top 5 prodotti per ogni condizione meteo, ordinati per vendite totali: 
SELECT *
FROM (
    SELECT 
        weather_description,
        product_name,
        category,
        sub_category,
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        RANK() OVER (PARTITION BY weather_description ORDER BY SUM(sales) DESC) AS rank_within_weather
    FROM sales_data.sales_weather_clean
    WHERE weather_description IS NOT NULL
    GROUP BY weather_description, product_name, category, sub_category
) ranked_products
WHERE rank_within_weather <= 5
ORDER BY weather_description, total_sales DESC;


-- QUERY 2: Most profitable temperature/humidity range Trovare la combinazione di temperatura e umidità che porta le maggiori vendite totali.    

SELECT 
    ROUND(temperature, 0) AS temp_rounded,
    ROUND(humidity, 0) AS humidity_rounded,
    COUNT(*) AS num_orders,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit
FROM sales_data.sales_weather_clean
WHERE temperature IS NOT NULL AND humidity IS NOT NULL
GROUP BY temp_rounded, humidity_rounded
ORDER BY total_sales DESC
LIMIT 10;

-- temperature from 268 to 299 , humidity from 48 to 86 
--  🌦️ QUERY 3: Weather conditions for the best-performing product Trova le condizioni meteo medie quando si vende il prodotto con più vendite totali.

-- Step 1: Identifica il best seller

WITH best_seller AS (
    SELECT product_name
    FROM sales_data.sales_weather_clean
    GROUP BY product_name
    ORDER BY SUM(sales) DESC
    LIMIT 1
)

-- Step 2: Calcola meteo medio per quel prodotto
SELECT 
    b.product_name,
    AVG(temperature) AS avg_temp,
    AVG(humidity) AS avg_humidity,
    weather_description,
    COUNT(*) AS order_count,
    SUM(sales) AS total_sales
FROM sales_data.sales_weather_clean s
JOIN best_seller b ON s.product_name = b.product_name
WHERE temperature IS NOT NULL AND humidity IS NOT NULL
GROUP BY b.product_name, weather_description
ORDER BY total_sales DESC;

--  🏙️ QUERY 4: Migliori città per vendite in condizioni meteo favorevoli In quali città si vendono di più i prodotti in condizioni meteo ottimali?


--   temperature ottimali : 268 to 299 , humidity from 48 to 86 ,  
SELECT 
    city,
    category,
    sub_category,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit,
    COUNT(*) AS num_orders
FROM sales_data.sales_weather_clean
WHERE temperature BETWEEN 268 AND 299
  AND humidity BETWEEN 48 AND 86
GROUP BY city, category, sub_category
ORDER BY total_sales DESC
LIMIT 10;


-- 🧮 QUERY 5: Relazione tra meteo e sub_category Quali sub-categorie sono più vendute con specifiche condizioni meteo?

SELECT 
    weather_description,
    sub_category,
    AVG(temperature) AS avg_temp,
    AVG(humidity) AS avg_humidity,
    SUM(sales) AS total_sales,
    COUNT(*) AS num_orders
FROM sales_data.sales_weather_clean
GROUP BY weather_description, sub_category
ORDER BY total_sales DESC
LIMIT 20;


-- analisi per città e regione ::  Analisi per umidità alta (>70%), con dettaglio per città e regione

SELECT 
    region,
    city,
    category,
    sub_category,
    COUNT(*) AS num_orders,
    SUM(quantity) AS total_quantity,
    SUM(sales) AS total_sales
FROM sales_data.sales_weather_clean
WHERE humidity > 70
GROUP BY region, city, category, sub_category
ORDER BY region, city, total_sales DESC;


-- Analisi condizioni “ideali” per SALES  (temperatura tra 268 e 299, umidità tra 50 e 60)

SELECT 
    region,
    city,
    category,
    sub_category,
    COUNT(*) AS num_orders,
    SUM(sales) AS total_sales,
    ROUND(AVG(temperature), 1) AS avg_temp,
    ROUND(AVG(humidity), 1) AS avg_humidity
FROM sales_data.sales_weather_clean
WHERE temperature BETWEEN 226 AND 299
  AND humidity BETWEEN 50 AND 60
GROUP BY region, city, category, sub_category
ORDER BY total_sales DESC;

