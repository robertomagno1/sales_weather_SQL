-- =====================================================================================
-- PROJECT: Sales & Weather Analysis
-- DATABASE: sales1
-- DESCRIPTION:
--   This project integrates two distinct datasets—sales data and 
--   weather conditions—with the goal of enabling insightful queries 
--   that correlate weather patterns with sales performance.
--   
--   The primary objective is to understand how different weather 
--   conditions impact sales, such as whether rainy days reduce 
--   customer purchases, or if certain products sell more in specific 
--   weather scenarios.
--
--   This part of the schema defines the `orders` table, which contains 
--   detailed sales records including customer information, product 
--   details, and transactional metrics.
--
--   A separate `weather` table will later be introduced and linked 
--   to this table via shared temporal (and potentially geographic) 
--   fields like `order_date` and `city` or `state`, allowing us 
--   to perform meaningful joins and analyses.
-- =====================================================================================



-- Create the project database if it doesn't already exist
CREATE DATABASE IF NOT EXISTS sales_data;

-- Create the raw orders table to hold imported sales data
CREATE TABLE IF NOT EXISTS sales_data.orders_raw (
    row_id INT,  -- Internal unique identifier for each row
    order_id VARCHAR(50),  -- Unique order code (may repeat if multiple items in one order)
    order_date VARCHAR(20),   -- Stored as string for format compatibility during import
    ship_date VARCHAR(20),   -- Same as above for shipping date
    ship_mode VARCHAR(50),  -- How the order was shipped (e.g., Standard Class)
    customer_id VARCHAR(50),  -- Unique ID per customer
    customer_name VARCHAR(100),  -- Full name of the customer
    segment VARCHAR(50),  -- Market segment (e.g., Consumer, Corporate)
    country VARCHAR(50),  -- Customer country
    city VARCHAR(50),  -- Customer city (used for joining with weather data)
    state VARCHAR(50),  -- Customer state/province
    postal_code VARCHAR(20),  -- String format for broader compatibility
    region VARCHAR(50),  -- Sales region (e.g., West, South)
    product_id VARCHAR(50),  -- Unique identifier for the product
    category VARCHAR(50),  -- Main product category
    sub_category VARCHAR(50),  -- More specific sub-category
    product_name VARCHAR(200),  -- Full descriptive name of the product
    sales DECIMAL(10,2),  -- Amount of money from the sale
    quantity INT,  -- Number of items sold
    discount DECIMAL(5,2),  -- Discount applied (e.g., 0.2 for 20%)
    profit DECIMAL(10,2)  -- Profit from the transaction
);




 -- =======================================================================
-- SECTION: Weather Table Creation and Uniqueness Check for Order IDs
-- DATABASE: sales_data
-- DESCRIPTION:
--   This section begins with a query to inspect the raw sales orders and
--   to verify whether the ⁠ order_id ⁠ can be used as a unique primary key.
--   It turns out that multiple products can be sold under the same order,
--   so ⁠ order_id ⁠ cannot be unique in this context.
--
--   Then, we create three key tables that store weather data:
--     1. temperature
--     2. humidity
--     3. description
--   Each table uses a composite primary key based on ⁠ date ⁠ and ⁠ city ⁠,
--   allowing for accurate matching between weather records and sales events.
--   Indexes are added to improve query performance during joins and filtering.


-- =======================================================================





-- Inspect the contents of the raw orders
SELECT * FROM sales1.orders_raw;

-- Check for non-unique order_ids
-- This reveals whether a single order ID may appear more than once
-- (e.g., when a customer buys multiple items in one purchase)
-- PRIMARY KEYS must be UNIQUE and NOT NULL, so we validate this here
SELECT order_id, COUNT(*) AS cnt
FROM sales_data.sales_weather
GROUP BY order_id
HAVING cnt > 1
ORDER BY cnt DESC;



-- =======================================================================
-- Temperature Table
-- Captures daily average temperature data for each city
-- Composite key ensures uniqueness for (date, city) combinations
-- Indexes support fast lookup by city or date
-- =======================================================================

CREATE TABLE temperature (
    date DATE NOT NULL,
    city VARCHAR(50) NOT NULL,
    temperature DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (date, city),
    INDEX idx_city (city),
    INDEX idx_date (date)
);

-- Humidity Table
-- Stores relative humidity readings per city per day
-- Same structure and indexing strategy as the temperature table
CREATE TABLE humidity (
    date DATE NOT NULL,
    city VARCHAR(50) NOT NULL,
    humidity DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (date, city),
    INDEX idx_city (city),
    INDEX idx_date (date)
);

-- Weather Description Table
-- Records qualitative descriptions like 'Sunny', 'Rainy', etc.
-- Follows same schema structure for consistency
CREATE TABLE description (
    date DATE NOT NULL,
    city VARCHAR(50) NOT NULL,
    description VARCHAR(50),
    PRIMARY KEY (date, city),
    INDEX idx_city (city),
    INDEX idx_date (date)
);







-- =======================================================================
-- SECTION: Weather Data Integration and Join with Sales Orders
-- DESCRIPTION:
--   In this section, we consolidate weather data from multiple sources—
--   temperature, humidity, and description—into a single structure.
--   This enables us to seamlessly join weather conditions with sales records
--   and perform analysis based on both environmental and commercial factors.
-- =======================================================================

-- Create a combined view (for quick reference) that joins weather components
-- Note: This query could be turned into a materialized table if performance is needed
-- CREATE VIEW sales_data.combined_weather AS
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

-- Create a materialized version of the above using LEFT JOINs
-- Purpose: Ensure we don't lose temperature rows even if humidity or description is missing
CREATE TABLE combined_weather1 AS
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

-- Count how many records we successfully aggregated into the combined table
SELECT COUNT(*) FROM combined_weather1;

-- ⚡ Benchmark query performance
-- This helps us evaluate how efficiently queries run on the combined table
EXPLAIN ANALYZE SELECT * FROM combined_weather1;

-- =======================================================================
-- SECTION: Join Sales with Weather
-- DESCRIPTION:
--   Here, we create a new table `sales_weather` by joining weather data
--   to sales orders using city and date. Since order dates are stored
--   as strings in the `orders_raw` table, we convert them on the fly.
--   This join enables advanced sales analysis based on daily weather.
-- =======================================================================

CREATE TABLE sales_weather AS 
SELECT 
  o.*, 
  w.temperature, 
  w.humidity, 
  w.description
FROM 
  sales_data.orders_raw o
INNER JOIN sales_data.weather_condition w 
  ON STR_TO_DATE(o.order_date, '%d/%m/%y') = w.date  -- Convert string to date for accurate join
  AND o.city = w.city;

-- Performance Check: Time it takes to read from the full joined table
EXPLAIN ANALYZE SELECT * FROM sales_weather;  -- ~0.15 sec

-- How many rows were generated from the join?
SELECT COUNT(*) FROM sales_weather;  -- Good to validate row integrity







-- =======================================================================
-- QUERY 1: Temperature and Humidity Distribution per City
-- Purpose: Understand average climate conditions in each city.
-- =======================================================================
SELECT 
    City,                               -- Name of the city (used for grouping)
    AVG(temperature) AS avg_temp,       -- Average temperature in that city
    AVG(humidity) AS avg_humidity       -- Average humidity in that city
FROM 
    sales_weather                       -- Source table combining sales and weather data
GROUP BY 
    City;                               -- Grouping results per city

-- =======================================================================
-- QUERY 2: Average Sales by Region and City
-- Purpose: Identify cities and regions with the highest average sales.
-- =======================================================================
SELECT 
    Region,                             -- Geographic region (e.g., West, East)
    City,                               -- Specific city within the region
    AVG(Sales) AS avg_sales             -- Average sales amount for each city
FROM 
    sales_weather
GROUP BY 
    Region, City;                       -- Grouping by both region and city

-- =======================================================================
-- QUERY 3: Total Sales and Profit by Region and State
-- Purpose: Rank regions/states by overall sales and profit.
-- =======================================================================
SELECT 
    Region,                             -- High-level geographic zone
    State,                              -- State within the region
    SUM(Sales) AS Total_Sales,          -- Total sales value in the state
    SUM(Profit) AS Total_Profit         -- Total profit generated in the state
FROM 
    sales_weather
GROUP BY 
    Region, State                       -- Aggregating by region and state
ORDER BY 
    Total_Sales DESC;                   -- Sorting to show highest sales first
    
    
    

-- =======================================================================
-- QUERY 4: Most Profitable Customers
-- Purpose: Identify which customers generated the highest total profit.
-- =======================================================================
SELECT 
    Customer_ID,                        -- Unique identifier for each customer
    Customer_Name,                      -- Full name of the customer
    SUM(Profit) AS Total_Profit         -- Total profit generated from all their purchases
FROM 
    sales_weather
GROUP BY 
    Customer_ID, Customer_Name          -- Grouping to aggregate profit by customer
ORDER BY 
    Total_Profit DESC;                  -- Sorting to get the most profitable customers at the top


-- =======================================================================
-- QUERY 5: Most Profitable Customers (with Sales Insight)
-- Purpose: Extended version of Query 4, including total sales alongside profit.
-- =======================================================================
SELECT 
    Customer_ID,                        -- Customer identifier
    Customer_Name,                      -- Customer full name
    SUM(Sales) AS Total_Sales,          -- Total value of purchases made by the customer
    SUM(Profit) AS Total_Profit         -- Corresponding profit generated
FROM 
    sales_weather
GROUP BY 
    Customer_ID, Customer_Name          -- Aggregated by customer
ORDER BY 
    Total_Profit DESC;                  -- Customers ranked by profit


-- =======================================================================
-- QUERY 6: Product Performance by Segment, Category, and Sub-Category
-- Purpose: Analyze how different product types perform within each market segment.
-- =======================================================================
SELECT 
    Segment,                            -- Market segment (e.g., Consumer, Corporate)
    Category,                           -- Product category (e.g., Office Supplies)
    Sub_Category,                       -- More detailed product group (e.g., Binders)
    COUNT(Order_ID) AS Number_of_Orders, -- Number of orders placed for this sub-category
    SUM(Sales) AS Total_Sales,          -- Total sales revenue
    SUM(Profit) AS Total_Profit         -- Total profit from these products
FROM 
    sales_weather
GROUP BY 
    Segment, Category, Sub_Category     -- Aggregating at 3 levels: segment > category > sub-category
ORDER BY 
    Segment, Total_Sales DESC;          -- Sorting results by segment and top-selling items
    
    




-- =======================================================================
-- QUERY 7: Weather Conditions on High-Sales Days
-- Purpose: Identify what kind of weather was present in cities on days
--          when total sales exceeded $1000.
--          This helps explore potential correlations between good weather
--          and increased purchasing activity.
-- =======================================================================


SELECT DISTINCT 
    o.city,                            -- Name of the city where the sales occurred
    o.order_date,                      -- Original order date (still in string format here)
    d.description                      -- Weather description on that day (e.g., Clear, Rainy)
FROM 
    sales_data.orders_raw o            -- Sales data (raw, unoptimized)
JOIN 
    sales_data.description d           -- Weather descriptions table
    ON o.city = d.city                 -- Match records by city
    AND STR_TO_DATE(o.order_date, '%m/%d/%Y') = d.date  
                                       -- Convert order_date string to DATE and match with weather date
GROUP BY 
    o.city, o.order_date, d.description  
                                       -- Group to allow aggregation for HAVING clause
HAVING 
    SUM(o.sales) > 1000;               -- Filter to only include days with total sales > $1000




-- =======================================================================
-- QUERY 8: Top-Selling Products on Sunny Days per Region
-- Purpose:
--   This query identifies the single best-selling product (by total sales)
--   for each region, but only considering days where the weather was sunny 
--   (specifically: "sky is clear").
--
--   It uses Common Table Expressions (CTEs) and a window function to rank 
--   products within each region.
-- =======================================================================

-- 1: Define CTE 'sunny_orders' to filter sales only on sunny days
WITH sunny_orders AS (
    SELECT 
        o.region,                          -- Geographic region of the order
        o.product_name,                    -- Name of the product sold
        SUM(o.sales) AS total_sales        -- Aggregate sales per product and region
    FROM 
        sales_data.orders_raw o
    JOIN 
        sales_data.description d           -- Join with weather description
        ON o.city = d.city                 -- Match on city
        AND STR_TO_DATE(o.order_date, '%m/%d/%Y') = d.date
                                           -- Convert string date and match with weather date
    WHERE 
        d.description LIKE '%sky is clear%'  -- Only consider "sunny" days
    GROUP BY 
        o.region, o.product_name           -- Group by region and product for aggregation
),

-- 2: Rank products within each region based on total_sales
ranked_products AS (
    SELECT 
        *,  -- All fields from sunny_orders
        RANK() OVER (
            PARTITION BY region 
            ORDER BY total_sales DESC
        ) AS rnk                          -- Ranking products within each region (1 = top seller)
    FROM 
        sunny_orders
)

-- 3: Select only the top product per region (rank 1)
SELECT 
    region, 
    product_name, 
    total_sales
FROM 
    ranked_products
WHERE 
    rnk = 1;                              -- Keep only the top-selling product per region
    
    



-- Customers whose average order profit is below average in their region

-- =======================================================================
-- QUERY 9: Customers Whose Average Profit Is Below the Regional Average
-- Purpose:
--   This query identifies customers whose average order profit is 
--   lower than the average profit for their region.
--
--   It uses two Common Table Expressions (CTEs):
--   1. To compute the average profit per region.
--   2. To compute the average profit per customer.
--   Then it compares them to find underperforming customers.
-- =======================================================================

-- Step 1: CTE to calculate the average profit per region
WITH regional_avg_profit AS (
    SELECT 
        region,                               -- Geographic region
        AVG(profit) AS avg_profit_region      -- Average profit in that region
    FROM 
        sales_data.orders_raw
    GROUP BY 
        region                                -- One row per region
),

-- Step 2: CTE to calculate the average profit per customer (within their region)
customer_avg_profit AS (
    SELECT 
        customer_id,                          -- Unique identifier for the customer
        customer_name,                        -- Full name of the customer
        region,                               -- Region the customer belongs to
        AVG(profit) AS avg_profit_customer    -- Customer's average profit per order
    FROM 
        sales_data.orders_raw
    GROUP BY 
        customer_id, customer_name, region    -- Grouping to avoid duplicates
)

-- Step 3: Final selection of underperforming customers
SELECT 
    c.customer_id,                            -- Customer ID
    c.customer_name,                          -- Customer full name
    c.region,                                 -- Their region
    c.avg_profit_customer                     -- Their average profit
FROM 
    customer_avg_profit c                     -- From customer's average profit table
JOIN 
    regional_avg_profit r 
    ON c.region = r.region                    -- Join with regional averages
WHERE 
    c.avg_profit_customer < r.avg_profit_region
											
											  -- Filter: customers earning below the regional average
ORDER BY 
    avg_profit_region DESC;                   -- Sort by region's profit (most profitable at top)


-- =======================================================================
-- QUERY 10: Monthly Sales and Average Temperature per City (Year: 2014)
-- Purpose:
--   This query analyzes how sales and temperatures fluctuate monthly
--   in different cities during the year 2014. 
--   It combines sales and weather data using a common date and location,
--   and computes total monthly sales and average temperature per city.
--
--   It is useful for identifying seasonal trends and weather-related 
--   influences on sales.
-- =======================================================================

-- Step 1: Create a CTE to extract monthly metrics
WITH monthly_data AS (
    SELECT 
        o.city,                                              -- City of the order
        MONTH(STR_TO_DATE(o.order_date, '%m/%d/%Y')) AS month,  
                                                             -- Extract numeric month from string-formatted date
        SUM(o.sales) AS total_sales,                         -- Total sales in that month
        AVG(t.temperature) AS avg_temp                       -- Average temperature in that month
    FROM 
        sales_data.orders_raw o
    JOIN 
        sales_data.temperature t                             -- Join with temperature data
        ON o.city = t.city                                   -- Match by city
        AND STR_TO_DATE(o.order_date, '%m/%d/%Y') = t.date   -- Convert and match order date with temperature date
    WHERE 
        YEAR(STR_TO_DATE(o.order_date, '%m/%d/%Y')) = 2014    -- Filter only for the year 2014
    GROUP BY 
        o.city, MONTH(STR_TO_DATE(o.order_date, '%m/%d/%Y'))  -- Group by city and month
)

-- Step 2: Select from the CTE and order the result
SELECT 
    *                                                       -- Show all columns: city, month, total_sales, avg_temp
FROM 
    monthly_data
ORDER BY 
    city, month;                                            -- Sort alphabetically by city and chronologically by month
    
    
 
 
 
 
 
 
-- ============================================================================
-- DATE FORMAT OPTIMIZATION: Adding Permanent DATE Columns to orders_raw Table
-- Purpose:
--   The original dataset stored dates as strings (VARCHAR), which is inefficient
--   and problematic for date operations (like filtering, joins, ordering).
-- 
--   This block adds proper DATE columns and populates them using STR_TO_DATE,
--   which allows optimized and error-proof queries on temporal data.
-- ============================================================================

-- Add a new column for properly formatted order dates
ALTER TABLE sales_data.orders_raw 
ADD COLUMN order_date_proper DATE;

-- Populate the new column by converting existing string dates 
-- Format used: '%d/%m/%y' (example: '25/12/14' → 2014-12-25)
UPDATE sales_data.orders_raw 
SET order_date_proper = STR_TO_DATE(order_date, '%d/%m/%y');

-- Add a new column for proper ship dates as well
ALTER TABLE orders_raw 
ADD COLUMN ship_date_proper DATE;

-- [CORRECTION] Remove mistakenly named column (if created during tests)
ALTER TABLE orders_raw
DROP COLUMN shiop_date_proper;

-- Populate the new shipping date column by converting the string-based field
UPDATE orders_raw 
SET ship_date_proper = STR_TO_DATE(ship_date, '%d/%m/%y');


-- View the updated records to verify proper date conversion
SELECT * 
FROM sales_data.orders_raw;

-- ============================================================================
-- VIEW CREATION: Simplified and Cleaned Orders View
-- Purpose:
--   This view exposes only clean and necessary fields from the raw orders table,
--   using the newly formatted date columns for reliable analysis and joins.
--   It's useful for analysts to work on clean data without affecting raw inputs.
-- ============================================================================

CREATE OR REPLACE VIEW sales_data.orders_raw_view AS
SELECT 
    row_id,
    order_id,
    order_date_proper,          -- Clean and properly typed order date
    ship_date_proper,           -- Clean and properly typed shipping date
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
FROM 
    sales_data.orders_raw;

UPDATE orders_raw 
SET ship_date_proper = STR_TO_DATE(ship_date, '%d/%m/%y');






-- ============================================================================
-- TABLE: sales_weather_clean
-- PROJECT: Sales + Weather Data Integration
-- PURPOSE:
--   This table is an optimized, cleaned, and enriched version of the original 
--   sales-weather joined dataset. It is designed to enable faster, more reliable 
--   analytical queries by ensuring:
--     - Correct data types (e.g., proper DATE fields)
--     - Integrity constraints for data consistency
--     - Strategic indexes for performance optimization
-- ============================================================================

CREATE TABLE IF NOT EXISTS sales_data.sales_weather_clean (
    row_id VARCHAR(50) NOT NULL,                  -- Internal row identifier
    order_id VARCHAR(50) NOT NULL,                -- Sales order ID (can repeat for multi-product orders)
    order_date DATE NOT NULL,                     -- Properly formatted order date
    ship_date DATE,                               -- Properly formatted shipping date
    ship_mode VARCHAR(50),                        -- Shipping method (e.g., Standard Class)
    customer_id VARCHAR(50) NOT NULL,             -- Unique customer identifier
    customer_name VARCHAR(100) NOT NULL,          -- Full name of the customer
    segment VARCHAR(50),                          -- Customer segment (e.g., Consumer, Corporate)
    country VARCHAR(50) NOT NULL,                 -- Country of customer (assumed to be consistent)
    city VARCHAR(50) NOT NULL,                    -- City of customer (used to join with weather data)
    state VARCHAR(50),                            -- State/Province
    postal_code VARCHAR(20),                      -- Postal code (supports international formats)
    region VARCHAR(50),                           -- Region classification
    product_id VARCHAR(50) NOT NULL,              -- Unique product identifier
    category VARCHAR(50),                         -- Product category (e.g., Office Supplies)
    sub_category VARCHAR(50),                     -- More specific product category
    product_name VARCHAR(200),                    -- Product full name
    sales DECIMAL(10,2) NOT NULL,                 -- Total sales amount
    quantity INT NOT NULL CHECK (quantity > 0),   -- Quantity must be > 0
    discount DECIMAL(5,2) CHECK (discount >= 0 AND discount <= 1),  -- Discount percentage (0 to 1)
    profit DECIMAL(10,2),                         -- Profit from this transaction

    -- Weather-related attributes
    temperature DECIMAL(10,2),                    -- Recorded temperature on the order date
    humidity DECIMAL(10,2),                       -- Recorded humidity on the order date
    weather_description VARCHAR(50),              -- Textual weather condition (e.g., "Sky is Clear")

    -- PERFORMANCE OPTIMIZATION: Adding indexes for frequent filters and joins
    INDEX idx_order_id (order_id),
    INDEX idx_customer (customer_id),
    INDEX idx_product (product_id),
    INDEX idx_city (city),
    INDEX idx_order_date (order_date),
    INDEX idx_state (state),
    INDEX idx_region (region)
);

-- ============================================================================
-- POPULATE TABLE: Load Cleaned and Formatted Data
-- PURPOSE:
--   We populate the new optimized table from the temporary table ⁠ sales_weather ⁠,
--   making sure to convert the ⁠ order_date ⁠ and ⁠ ship_date ⁠ from string to DATE format,
--   and rename the weather field to a more readable name (⁠ weather_description ⁠).
-- ============================================================================

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
    STR_TO_DATE(order_date, '%d/%m/%y'),    -- Convert string to proper DATE for querying
    STR_TO_DATE(ship_date, '%d/%m/%y'),     -- Same for shipping date
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
    description AS weather_description       -- Renaming for clarity
FROM sales_data.sales_weather;





-- ============================================================================
-- 🔍 DATA VALIDATION & CLEANING: sales_weather_clean
-- PURPOSE:
--   This section focuses on verifying data quality within the optimized table 
--   ⁠ sales_weather_clean ⁠. The goals are:
--     1. Identify and handle duplicate records (e.g., same ⁠ row_id ⁠)
--     2. Ensure there are no NULLs in critical columns like ⁠ row_id ⁠
--     3. Investigate why ⁠ order_id ⁠ cannot be used as a PRIMARY KEY
-- ============================================================================

-- 🔁 STEP 1: Check for duplicate row_id values
--    While ⁠ row_id ⁠ should ideally be unique, this check helps confirm if 
--    accidental duplication has occurred (e.g., due to joins or multiple inserts).
SELECT 
    row_id, 
    COUNT(*) AS duplicate_count
FROM sales_data.sales_weather_clean
GROUP BY row_id
HAVING COUNT(*) > 1
LIMIT 10;

-- 🧹 INSIGHT: Only 2 duplicate row_ids were found.
--    These should be manually reviewed and removed if necessary to ensure data consistency.

-- 🚫 STEP 2: Check for NULL values in the ⁠ row_id ⁠ field
--    We want to confirm that all rows have a valid row identifier.
SELECT COUNT(*) 
FROM sales_data.sales_weather_clean 
WHERE row_id IS NULL;

-- ✅ RESULT: Zero NULL values were found. This means all rows have valid identifiers.

-- ❌ STEP 3: Remove the specific rows identified as duplicates
--    (Note: Not shown here, but you would use DELETE with a WHERE condition 
--    or use ROW_NUMBER in a CTE to retain only the first instance.)

-- ============================================================================
-- ⚙️ PERFORMANCE TEST: Evaluate execution speed of full table scan
--    The goal is to ensure that our optimizations (e.g., indexing) are effective.
-- ============================================================================

-- 🚀 Measure the execution plan and timing for a full table read
EXPLAIN ANALYZE SELECT * FROM sales_weather_clean;

-- ✅ Example Result: 0.08 sec (previous version without indexes took nearly twice as long)

-- 📊 Quick check: How many rows are in the optimized table?
SELECT COUNT(*) FROM sales_weather_clean;

-- ============================================================================
-- 🔍 BONUS VALIDATION: Why ⁠ order_id ⁠ cannot be UNIQUE
-- CONTEXT:
--   In a real sales system, a single order (order_id) may contain multiple products.
--   Therefore, order_id can repeat across rows — it's not a good candidate for PRIMARY KEY.
-- ============================================================================

-- 🧠 Check how many order_ids appear more than once
SELECT 
    order_id, 
    COUNT(*) AS cnt
FROM sales_data.sales_weather
GROUP BY order_id
HAVING cnt > 1
ORDER BY cnt DESC;

-- 🔁 RESULT: Many order_ids appear multiple times.
--    Example: An order with 3 different items will result in 3 separate rows,
--    all sharing the same ⁠ order_id ⁠ but differing in ⁠ product_id ⁠ and details.
--    ➤ This is why we avoided setting ⁠ order_id ⁠ as a UNIQUE constraint or PRIMARY KEY.






-- ============================================================================
-- QUERY 11: Sub-Category Product Performance by Weather Type
-- PURPOSE:
--   This query analyzes how different weather conditions influence product 
--   performance at the sub-category level. By combining sales and weather data, 
--   we can identify which sub-categories perform best under specific 
--   weather descriptions (e.g., "Rain", "Sunny", etc.).
--
--   This kind of analysis can support data-driven decisions like:
--     - targeted promotions based on forecasted weather
--     - optimizing inventory during certain climate conditions
--     - identifying product trends tied to seasonal weather
-- ============================================================================

SELECT 
    w.description AS weather_type,          -- Describes the weather condition (e.g., "Cloudy", "Clear")
    o.sub_category,                         -- Product sub-category from sales data
    COUNT(*) AS order_count,                -- Number of orders in this sub-category under this weather type
    SUM(o.quantity) AS total_units,         -- Total quantity of units sold
    ROUND(SUM(o.sales), 2) AS total_sales   -- Total sales amount, rounded to 2 decimals
FROM 
    sales_data.orders_raw_view o            -- View of the cleaned orders table
JOIN (
    -- Subquery extracts only relevant columns from the weather dataset
    SELECT date, city, description 
    FROM sales_data.weather_condition
) w 
    ON o.order_date_proper = w.date         -- Match by date
    AND o.city = w.city                     -- Match by city
GROUP BY 
    w.description,                          -- Group by weather type
    o.sub_category                          -- And by product sub-category
ORDER BY 
    total_sales DESC                        -- Prioritize by highest-selling sub-categories
LIMIT 10;                                   -- Return only the top 10 combinations





-- ============================================================================
-- QUERY 12: Category-Level Product Performance by Weather Type
-- PURPOSE:
--   This query extends the analysis of product sales by weather condition, 
--   focusing on higher-level product categories (e.g., Furniture, Technology).
--
--   While Query 11 analyzed sub-categories, here we generalize to observe 
--   broader trends—such as whether entire product categories perform better 
--   under certain weather types.
--
--   This is useful to:
--     - Assess demand shifts by weather at the category level
--     - Identify weather-related purchasing behavior on a macro scale
--     - Guide strategic planning for marketing and logistics
-- ============================================================================

SELECT 
    w.description,                  -- Weather description (e.g., "Rain", "Snow", "Clear sky")
    o.category,                     -- High-level product category (e.g., "Technology", "Furniture")
    COUNT(*) AS orders,             -- Number of transactions (rows) matching the condition
    SUM(o.quantity) AS items_sold   -- Total quantity sold across all orders
FROM 
    sales_data.orders_raw_view o,   -- Main view containing cleaned order data
    (
        SELECT date, city, description 
        FROM sales_data.weather_condition
    ) w                             -- Inline subquery to extract weather info by date and city
WHERE 
    o.order_date_proper = w.date    -- Match sales with weather by date
    AND o.city = w.city             -- Match by city
GROUP BY 
    w.description, o.category       -- Group by weather condition and product category
ORDER BY 
    items_sold DESC;                -- Show categories with highest unit sales first
    
    
    
    
    

-- ============================================================================
-- QUERY 13: Top 5 Best-Selling Products per Weather Condition
-- PURPOSE:
--   This query identifies the top 5 products (in terms of total sales) 
--   for each distinct weather condition. It helps answer questions like:
--     - What do people buy most often when it's sunny, rainy, or foggy?
--     - Are there product trends tied to specific weather patterns?
--
--   The goal is to uncover weather-based purchasing preferences 
--   at the product level, useful for seasonal marketing or inventory planning.
--
--   This is accomplished using a window function (RANK) to assign 
--   product rankings within each weather group.
-- ============================================================================

SELECT *
FROM (
    SELECT 
        weather_description,                          -- e.g., "Clear sky", "Rain", etc.
        product_name,                                 -- Full name of the product
        category,                                     -- High-level product category
        sub_category,                                 -- More granular product category
        SUM(sales) AS total_sales,                    -- Total revenue for the product
        SUM(quantity) AS total_quantity,              -- Number of units sold
        RANK() OVER (
            PARTITION BY weather_description 
            ORDER BY SUM(sales) DESC
        ) AS rank_within_weather                      -- Rank products within each weather group
    FROM sales_data.sales_weather_clean
    WHERE weather_description IS NOT NULL             -- Exclude records without weather info
    GROUP BY 
        weather_description, product_name, 
        category, sub_category
) ranked_products
WHERE rank_within_weather <= 5                         -- Filter to top 5 products per weather type
ORDER BY 
    weather_description, total_sales DESC;            -- Final sorted output for clarity
    
    
    
-- ============================================================================
-- QUERY 14: Most Profitable Temperature and Humidity Combinations
-- PURPOSE:
--   This query investigates which specific combinations of temperature and 
--   humidity are associated with the highest total sales and profits.
--
--   The goal is to determine whether certain climate conditions are more 
--   conducive to increased purchasing behavior, which can be highly valuable 
--   for marketing, supply chain, or retail strategy planning.
--
-- STRATEGY:
--   - We round temperature and humidity to whole numbers to create manageable 
--     buckets (e.g., 22.4°C and 22.6°C both round to 22°C).
--   - We count the number of orders per unique (temp, humidity) pair.
--   - We calculate both total sales and total profit per group.
--   - We sort results by ⁠ total_sales ⁠ in descending order to find the best-performing ranges.
-- ============================================================================

SELECT 
    ROUND(temperature, 0) AS temp_rounded,       -- Round temperature to nearest integer
    ROUND(humidity, 0) AS humidity_rounded,      -- Round humidity to nearest integer
    COUNT(*) AS num_orders,                      -- Number of sales transactions in this climate
    SUM(sales) AS total_sales,                   -- Total revenue generated
    SUM(profit) AS total_profit                  -- Total profit generated
FROM sales_data.sales_weather_clean
WHERE 
    temperature IS NOT NULL AND 
    humidity IS NOT NULL                         -- Filter out any incomplete climate data
GROUP BY 
    temp_rounded, 
    humidity_rounded                             -- Group by each (temperature, humidity) pair
ORDER BY 
    total_sales DESC                             -- Show most profitable combinations first
LIMIT 100;                                       -- Limit to top 100 rows for readability


-- temperature from 268 to 299 , humidity from 48 to 86 


-- ============================================================================
-- QUERY 15: Weather Conditions for the Best-Performing Product
-- PURPOSE:
--   This query identifies the best-selling product (based on total sales),
--   and then analyzes the average weather conditions (temperature, humidity,
--   and description) under which it was sold.
--
--   The goal is to discover possible weather-related trends or correlations 
--   for the top-performing product. This is useful for forecasting, 
--   targeted promotions, or climate-aware stock management.
-- ============================================================================

-- STEP 1: Identify the best-selling product overall
WITH best_seller AS (
    SELECT product_name
    FROM sales_data.sales_weather_clean
    GROUP BY product_name
    ORDER BY SUM(sales) DESC  -- Order products by total sales (descending)
    LIMIT 1                   -- Pick the top 1 product
)

-- STEP 2: Analyze average weather for the best-seller
SELECT 
    b.product_name,                            -- Return the product name (from CTE)
    AVG(temperature) AS avg_temp,              -- Average temperature during its sales
    AVG(humidity) AS avg_humidity,             -- Average humidity during its sales
    weather_description,                       -- Weather description (e.g. clear sky, rain)
    COUNT(*) AS order_count,                   -- Number of orders placed for the product
    SUM(sales) AS total_sales                  -- Total sales value for this product
FROM sales_data.sales_weather_clean s
JOIN best_seller b 
  ON s.product_name = b.product_name           -- Only include rows for the best-selling product
WHERE 
    temperature IS NOT NULL AND 
    humidity IS NOT NULL                       -- Filter out incomplete weather data
GROUP BY 
    b.product_name, 
    weather_description                        -- Analyze by weather description
ORDER BY 
    total_sales DESC;                          -- Highlight the most profitable weather types



-- ============================================================================
-- QUERY 16: Top Cities for Sales Under Favorable Weather Conditions
--
-- OBJECTIVE:
--   This query identifies the cities where the highest volume of sales occurred 
--   under what we define as "optimal" weather conditions:
--     - Temperature between 268K and 299K (~ -5°C to ~26°C)
--     - Humidity between 48% and 86%
--
--   The goal is to determine which cities and product categories perform best
--   in comfortable weather. These insights are valuable for marketing,
--   local promotions, and weather-aware demand forecasting.
-- ============================================================================

SELECT 
    city,                                 -- Name of the city where the sale happened
    category,                             -- Broad product category (e.g., Technology, Furniture)
    sub_category,                         -- More detailed product grouping
    SUM(sales) AS total_sales,            -- Total revenue generated under these conditions
    SUM(profit) AS total_profit,          -- Total profit generated
    COUNT(*) AS num_orders                -- Number of transactions made
FROM sales_data.sales_weather_clean
WHERE temperature BETWEEN 268 AND 299     -- Filter for the "optimal" temperature range
  AND humidity BETWEEN 48 AND 86          -- Filter for the "optimal" humidity range
GROUP BY city, category, sub_category     -- Group results by city and product type
ORDER BY total_sales DESC                 -- Rank by highest revenue
LIMIT 10;                                 -- Show only the top 10 best-performing results




-- ============================================================================
-- QUERY 17: Weather vs Sub-Category Performance
--
-- OBJECTIVE:
--   This query investigates the relationship between different weather 
--   conditions (e.g., clear sky, rain) and the performance of specific 
--   product sub-categories.
--
--   By analyzing which sub-categories perform best under certain weather 
--   descriptions, this insight can be used for:
--     - Weather-driven marketing campaigns
--     - Demand forecasting based on climate
--     - Strategic stocking of weather-sensitive products
-- ============================================================================

SELECT 
    weather_description,                     -- Type of weather (e.g., Rain, Clear Sky)
    sub_category,                            -- Specific product sub-category (e.g., Chairs, Phones)
    AVG(temperature) AS avg_temp,            -- Average temperature during sales of this sub-category
    AVG(humidity) AS avg_humidity,           -- Average humidity during those sales
    SUM(sales) AS total_sales,               -- Total revenue generated for this sub-category
    COUNT(*) AS num_orders                   -- Number of orders placed under this weather condition
FROM sales_data.sales_weather_clean
GROUP BY 
    weather_description,                     -- Group by weather type
    sub_category                             -- And by product sub-category
ORDER BY 
    total_sales DESC                         -- Focus on the highest-selling combinations
LIMIT 20;                                    -- Show top 20 most successful weather/sub-category pairs





-- ============================================================================
-- QUERY 18: Sales Analysis by Region and City in High Humidity Conditions
--
-- OBJECTIVE:
--   This query investigates how sales behave in locations where humidity 
--   exceeds 70%. By grouping data by region, city, category, and sub-category,
--   we can identify:
--     - Which cities and regions remain profitable under humid weather
--     - Which product types (at category and sub-category level) are in demand
--     - Potential regional trends for weather-adaptive sales strategy
--
-- USE CASES:
--   This is useful for:
--     - Adjusting inventory and logistics in tropical or coastal areas
--     - Targeting ads for weather-specific products (e.g., fans, moisture-proof goods)
--     - Understanding how humidity affects customer behavior
-- ============================================================================

SELECT 
    region,                                  -- Broad geographical region (e.g., West, South)
    city,                                    -- Specific city name
    category,                                -- Product category (e.g., Furniture, Technology)
    sub_category,                            -- More granular product classification
    COUNT(*) AS num_orders,                  -- Total number of orders in these conditions
    SUM(quantity) AS total_quantity,         -- Total quantity of items sold
    SUM(sales) AS total_sales                -- Total sales value for each city/category combo
FROM sales_data.sales_weather_clean
WHERE humidity > 70                          -- Only include records with high humidity
GROUP BY 
    region, city, category, sub_category     -- Grouping by location and product type
ORDER BY 
    region, city, total_sales DESC;          -- Sort results by region, city, and descending sales
    


-- ============================================================================
-- QUERY 19: Sales Performance Under Ideal Weather Conditions
--
-- OBJECTIVE:
--   This query identifies the best-selling product categories and sub-categories 
--   in regions and cities that experience what are considered "ideal" weather conditions:
--     - Temperature between 226K and 299K (~ -47°C to 25.8°C) 
--     - Humidity between 50% and 60%
--
--   These ranges are used as proxies for temperate weather—neither too hot 
--   nor too humid—ideal for encouraging shopping or logistics.
--
-- USE CASES:
--   - Determine how mild weather conditions affect consumer behavior
--   - Understand if certain regions perform better due to climate
--   - Guide seasonal marketing campaigns or promotions
-- ============================================================================

SELECT 
    region,                                   -- Region (e.g., East, West, South, Central)
    city,                                     -- City name
    category,                                 -- Product category (e.g., Office Supplies)
    sub_category,                             -- Product sub-category (e.g., Binders, Chairs)
    COUNT(*) AS num_orders,                   -- Number of orders in "ideal" weather
    SUM(sales) AS total_sales,                -- Total sales in monetary value
    ROUND(AVG(temperature), 1) AS avg_temp,   -- Average temperature rounded to 1 decimal
    ROUND(AVG(humidity), 1) AS avg_humidity   -- Average humidity rounded to 1 decimal
FROM sales_data.sales_weather_clean
WHERE 
    temperature BETWEEN 226 AND 299           -- Ideal temperature range in Kelvin
  AND humidity BETWEEN 50 AND 60              -- Ideal humidity range
GROUP BY 
    region, city, category, sub_category      -- Group by location and product details
ORDER BY 
    total_sales DESC;                         -- Focus on the most profitable combinations first
    
    
    
    
    

-- ================================================================================
-- PERFORMANCE OPTIMIZATION: Sales Analysis under Clear Weather Conditions
--
-- GOAL:
--   Analyze the average sales per region specifically when the weather is 
--   described as 'sky is clear'. This helps assess if sales perform better 
--   in sunny, favorable conditions.
--
-- CHALLENGE:
--   The original query performs a full index scan on `sales_weather_clean`, 
--   evaluating thousands of rows but using only a portion (43%) of them, 
--   resulting in unnecessary resource usage and slower performance.
--
-- STRATEGY:
--   Use `EXPLAIN ANALYZE` to assess query cost and identify inefficiencies.
--   Then improve performance by creating targeted indexes.
-- ================================================================================

-- STEP 1: Performance Diagnostics with EXPLAIN ANALYZE
EXPLAIN ANALYZE
SELECT /*+ SET_VAR(max_execution_time=30000) */
    region,
    AVG(sales) AS avg_sales
FROM sales_data.sales_weather_clean
WHERE weather_description = 'sky is clear'
GROUP BY region;

-- SAMPLE PLAN OUTPUT:
-- -> Group aggregate: avg(sales_weather_clean.sales)
--    (cost=411 rows=4) (actual time=7.96..12.5 rows=4 loops=1)
--     -> Filter: (weather_description = 'sky is clear')
--        (cost=376 rows=352) (actual time=2.07..12.1 rows=1563 loops=1)
--         -> Index scan on sales_weather_clean using idx_region
--            (cost=376 rows=3515) (actual time=1.99..11.5 rows=3616 loops=1)

-- INTERPRETATION:
--   Although an index is used (`idx_region`), it's not selective enough.
--   The table reads over 3,600 rows but keeps only 1,563 (~43%) — not efficient.

-- STEP 2: Create a Composite Index to Improve Filtering Efficiency
--   This index helps by covering both the `weather_description` used in WHERE 
--   and the `region` used in GROUP BY.

CREATE INDEX idx_weather_region ON sales_weather_clean (weather_description, region);

-- FURTHER OPTIMIZATION:
--   Add `sales` to the index if you're aggregating it directly,
--   enabling index-only scans in some DBMS.

CREATE INDEX idx_weather_region_sales ON sales_weather_clean 
(weather_description, region, sales);

-- STEP 3: Run the Original Query Again After Index Creation
--   Expecting faster performance now due to the better use of filtered index.

EXPLAIN ANALYZE
SELECT 
    region,
    AVG(sales) AS avg_sales
FROM sales_weather_clean
WHERE weather_description = 'sky is clear'
GROUP BY region;

-- STEP 4: Force the Index (Optional - only if the DBMS does not pick the new index automatically)

-- This query hints to MySQL to use the index we just created
EXPLAIN ANALYZE
SELECT /*+ INDEX(swc idx_weather_region) */
    region,
    AVG(sales) AS avg_sales
FROM sales_weather_clean swc
WHERE weather_description = 'sky is clear'
GROUP BY region;

-- OPTIONAL: Create a View for Reusability
-- You could encapsulate this analysis in a view for further reporting

CREATE OR REPLACE VIEW avg_sales_by_clear_weather AS
SELECT 
    region,
    AVG(sales) AS avg_sales
FROM sales_weather_clean
WHERE weather_description = 'sky is clear'
GROUP BY region;

-- Now this is accessible via:
SELECT * FROM avg_sales_by_clear_weather;

