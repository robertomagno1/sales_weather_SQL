CREATE DATABASE IF NOT EXISTS sales1;


-- Select all:

USE sales1;

CREATE TABLE orders (
    row_id INT PRIMARY KEY,
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
    postal_code VARCHAR(20),  -- Changed to VARCHAR for international support
    region VARCHAR(50),
    product_id VARCHAR(50) NOT NULL,
    category VARCHAR(50),
    sub_category VARCHAR(50),
    product_name VARCHAR(200),
    sales DECIMAL(10,2) NOT NULL,
    quantity INT NOT NULL CHECK (quantity > 0),
    discount DECIMAL(5,2) CHECK (discount >= 0 AND discount <= 1),
    profit DECIMAL(10,2),
    UNIQUE (order_id),
    INDEX (customer_id),
    INDEX (product_id)
);


-- made for check the time range for this dataset 
SELECT 
    MIN(order_date) AS start_date,
    MAX(order_date) AS end_date
FROM 
    sales1.orders;
    
-- I create this table just to match datetime with the new dataset about weather;

CREATE TABLE dataset_5_years AS
SELECT *
FROM sales1.orders
WHERE order_date BETWEEN '2012-10-01' AND '2017-11-30';


select count(*) from sales1.dataset_5_years;


--  now itìs time to import the weather dataset ::


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

-- Optional: Combined weather table
CREATE TABLE weather (
    date DATE NOT NULL,
    city VARCHAR(50) NOT NULL,
    temperature DECIMAL(10,2),
    humidity DECIMAL(10,2),
    description VARCHAR(50),
    PRIMARY KEY (date, city),
    INDEX idx_city (city),
    INDEX idx_date (date)
);



-- JOIN all :

CREATE TABLE sales_weather_joined AS
SELECT
    s.*,
    t.temperature,
    h.humidity,
    d.description AS weather_condition
FROM 
    dataset_5_years s
LEFT JOIN 
    temperature t ON s.order_date = t.date AND s.city = t.city
LEFT JOIN 
    humidity h ON s.order_date = h.date AND s.city = h.city
LEFT JOIN 
    description d ON s.order_date = d.date AND s.city = d.city;



-- Improved Weather Data Matching Strategy

-- First create a combined view of all weather data
CREATE VIEW sales1.combined_weather AS
SELECT 
    t.date,
    t.city,
    t.temperature,
    h.humidity,
    d.description
FROM 
    sales1.temperature t
JOIN 
    sales1.humidity h ON t.date = h.date AND t.city = h.city
JOIN 
    sales1.description d ON t.date = d.date AND t.city = h.city;
    
    
drop table if exists city_state_reference ;

-- Create proper city-state reference table
CREATE TABLE IF NOT EXISTS sales1.city_state_reference (
    city VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    region VARCHAR(50),
    PRIMARY KEY (city, state)
);

-- Populate with correct data from orders table
INSERT IGNORE INTO sales1.city_state_reference (city, state, region)
SELECT DISTINCT 
    city, 
    state,
    region
FROM 
    sales1.orders
WHERE 
    city IS NOT NULL 
    AND state IS NOT NULL;
    
    

-- joined correctly , imputed values for weather as nearest as possible , imputation by stasticis ;

CREATE TABLE sales1.sales_weather_final AS
SELECT
    s.*,
    -- Temperature with fallback logic
    COALESCE(
        t.temperature,
        (SELECT AVG(t2.temperature) 
         FROM sales1.temperature t2
         JOIN sales1.city_state_reference c ON t2.city = c.city
         WHERE t2.date = s.order_date AND c.state = s.state),
        (SELECT AVG(t3.temperature)
         FROM sales1.temperature t3
         JOIN sales1.city_state_reference c ON t3.city = c.city
         WHERE t3.date = s.order_date AND c.region = s.region),
        (SELECT AVG(temperature) FROM sales1.temperature WHERE date = s.order_date)
    ) AS temperature,
    
    -- Humidity with fallback logic
    COALESCE(
        h.humidity,
        (SELECT AVG(h2.humidity) 
         FROM sales1.humidity h2
         JOIN sales1.city_state_reference c ON h2.city = c.city
         WHERE h2.date = s.order_date AND c.state = s.state),
        (SELECT AVG(h3.humidity)
         FROM sales1.humidity h3
         JOIN sales1.city_state_reference c ON h3.city = c.city
         WHERE h3.date = s.order_date AND c.region = s.region),
        (SELECT AVG(humidity) FROM sales1.humidity WHERE date = s.order_date)
    ) AS humidity,
    
    -- Weather description with fallback logic
    COALESCE(
        d.description,
        (SELECT d2.description 
         FROM sales1.description d2
         JOIN sales1.city_state_reference c ON d2.city = c.city
         WHERE d2.date = s.order_date AND c.state = s.state
         LIMIT 1),
        (SELECT d3.description
         FROM sales1.description d3
         JOIN sales1.city_state_reference c ON d3.city = c.city
         WHERE d3.date = s.order_date AND c.region = s.region
         LIMIT 1),
        (SELECT description FROM sales1.description WHERE date = s.order_date LIMIT 1)
    ) AS weather_condition
FROM 
    sales1.dataset_5_years s
LEFT JOIN 
    sales1.temperature t ON s.order_date = t.date AND s.city = t.city
LEFT JOIN 
    sales1.humidity h ON s.order_date = h.date AND s.city = h.city
LEFT JOIN 
    sales1.description d ON s.order_date = d.date AND s.city = d.city;
    
    
    
-- Add indexes to weather tables
ALTER TABLE sales1.temperature ADD INDEX idx_date_city (date, city);
ALTER TABLE sales1.humidity ADD INDEX idx_date_city (date, city);
ALTER TABLE sales1.description ADD INDEX idx_date_city (date, city);

-- Add indexes to final table
ALTER TABLE sales1.sales_weather_final 
ADD INDEX idx_order_date (order_date),
ADD INDEX idx_city (city),
ADD INDEX idx_state (state),
ADD INDEX idx_region (region);
    
-- verification query::

-- Check coverage statistics
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN temperature IS NOT NULL THEN 1 ELSE 0 END) AS temp_coverage,
    SUM(CASE WHEN humidity IS NOT NULL THEN 1 ELSE 0 END) AS humidity_coverage,
    SUM(CASE WHEN weather_condition IS NOT NULL THEN 1 ELSE 0 END) AS desc_coverage,
    ROUND(100*SUM(CASE WHEN temperature IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS temp_pct,
    ROUND(100*SUM(CASE WHEN humidity IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS humidity_pct,
    ROUND(100*SUM(CASE WHEN weather_condition IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*),1) AS desc_pct
FROM sales1.sales_weather_final;

-- Sample results
SELECT 
    order_date, city, state, region, 
    temperature, humidity, weather_condition
FROM sales1.sales_weather_final
LIMIT 20;
    
    
-- . Sales Performance Analysis

-- Top performing products by profit
SELECT 
    product_name, 
    category, 
    sub_category,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2) AS profit_margin
FROM 
    sales_weather_final
GROUP BY 
    product_name, category, sub_category
ORDER BY 
    total_profit DESC
LIMIT 10;

-- . Weather Impact on Sales
SELECT 
    weather_condition,
    COUNT(*) AS number_of_orders,
    ROUND(AVG(quantity), 2) AS avg_items_per_order,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(AVG(sales), 2) AS avg_order_value
FROM 
    sales_weather_final
WHERE 
    weather_condition IS NOT NULL
GROUP BY 
    weather_condition
ORDER BY 
    total_sales DESC;
    
-- 3. Seasonal Sales Trends

-- Monthly sales trends across years
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    COUNT(DISTINCT order_id) AS order_count
FROM 
    sales_weather_final
GROUP BY 
    YEAR(order_date), MONTH(order_date)
ORDER BY 
    year, month;
    
-- 4-- How temperature ranges affect sales
SELECT 
    CASE 
        WHEN temperature < 0 THEN 'Below 0°C'
        WHEN temperature BETWEEN 0 AND 10 THEN '0-10°C'
        WHEN temperature BETWEEN 10 AND 20 THEN '10-20°C'
        WHEN temperature BETWEEN 20 AND 30 THEN '20-30°C'
        WHEN temperature > 30 THEN 'Above 30°C'
        ELSE 'Unknown'
    END AS temp_range,
    COUNT(*) AS order_count,
    ROUND(AVG(sales), 2) AS avg_sale,
    ROUND(SUM(profit)/SUM(sales)*100, 2) AS profit_margin
FROM 
    sales_weather_final
WHERE 
    temperature IS NOT NULL
GROUP BY 
    temp_range
ORDER BY 
    temp_range;
    
-- 5 -- Customer segment performance
SELECT 
    segment,
    COUNT(DISTINCT customer_id) AS customer_count,
    COUNT(*) AS order_count,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(AVG(sales), 2) AS avg_order_value,
    ROUND(SUM(profit), 2) AS total_profit
FROM 
    sales_weather_final
GROUP BY 
    segment
ORDER BY 
    total_sales DESC;
    
-- 6. Shipping Efficiency : Shipping mode performance
SELECT 
    ship_mode,
    COUNT(*) AS order_count,
    ROUND(AVG(DATEDIFF(ship_date, order_date)), 2) AS avg_delivery_days,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit
FROM 
    sales_weather_final
WHERE 
    ship_date IS NOT NULL
GROUP BY 
    ship_mode
ORDER BY 
    avg_delivery_days;

-- 7. Regional Performance :  Sales by region and state
SELECT 
    region,
    state,
    COUNT(*) AS order_count,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2) AS profit_margin
FROM 
    sales_weather_final
GROUP BY 
    region, state
ORDER BY 
    region, total_sales DESC;
    
-- 8. Weather and Product Category Correlation   : How weather affects different product categories
SELECT 
    weather_condition,
    category,
    COUNT(*) AS order_count,
    ROUND(SUM(sales), 2) AS total_sales
FROM 
    sales_weather_final
WHERE 
    weather_condition IS NOT NULL
GROUP BY 
    weather_condition, category
ORDER BY 
    weather_condition, total_sales DESC;
    
-- 9. Discount Effectiveness Analysis :  How discounts affect sales and profitability
SELECT 
    CASE 
        WHEN discount = 0 THEN 'No discount'
        WHEN discount <= 0.2 THEN '0-20% discount'
        WHEN discount <= 0.4 THEN '20-40% discount'
        WHEN discount <= 0.6 THEN '40-60% discount'
        ELSE '60%+ discount'
    END AS discount_range,
    COUNT(*) AS order_count,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(SUM(profit)/SUM(sales)*100, 2) AS profit_margin
FROM 
    sales_weather_final
GROUP BY 
    discount_range
ORDER BY 
    discount_range;


-- 10. Combined Weather and Time Analysis : Sales performance by weather and day of week
SELECT 
    weather_condition,
    DAYNAME(order_date) AS day_of_week,
    COUNT(*) AS order_count,
    ROUND(SUM(sales), 2) AS total_sales
FROM 
    sales_weather_final
WHERE 
    weather_condition IS NOT NULL
GROUP BY 
    weather_condition, DAYNAME(order_date)
ORDER BY 
    weather_condition, 
    FIELD(DAYNAME(order_date), 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');
