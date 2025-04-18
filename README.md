# 🌦️ Sales & Weather Analysis Project.

	An SQL project to analyze the impact of weather on sales by integrating sales and weather data into a single optimized pipeline.

---

## 📌 Project goals.

- Integrate heterogeneous data (sales and weather) into a single relational database.
- Optimize SQL table structure with constraints and indexes.
- Perform advanced analysis on sales conditioned by weather parameters (temperature, humidity, weather conditions).
- Develop complex SQL queries (JOIN, nested, aggregations, multiple conditions).

---

## 🛠️ Technologies used.

- *SQL* (ANSI standard, tested on PostgreSQL and SQLite)
- *DBMS* (PostgreSQL, SQLite or other compatible)
- *Optional tools*: DBeaver, pgAdmin, SQLite Browser, Jupyter (for integrated execution via Python-SQL)

---

## 📁 Project structure


📦 project/
├── 📂 sql/
	├──  📜 00_create_tables.sql # Create raw tables of sales and weather
 	├──  📜 01_create_view_weather.sql # Create combined view of weather data
 	├── 📜 02_join_weather_sales.sql # Join weather data and sales
 	├── 📜 03_clean_optimized_table.sql # Create clean table with indexes and constraints
 	├── 📜 04_queries.sql # Advanced analysis queries.
 	├──  📜 README_queries.md # Detailed explanations queries 11-19
	├── 📜 README.md # This file.
└── 📜 schema_diagram.png # (optional) ER diagram of tables.


---

## 🧭 Step-by-Step - How to explore the project.

### 🔹 1. *Create tables*
Run 00_create_tables.sql to generate tables:
- orders_raw
- temperature
- humidity
- description

### 🔹 2. *Create weather view*
Run 01_create_view_weather.sql to get combined_weather , a view that combines temperature, humidity, and description by date and time.

### 🔹 3. *Join between weather and sales*.
Run 02_join_weather_sales.sql to merge sales data with weather data.

### 🔹 4. *Cleaning and optimization*
Run 03_clean_optimized_table.sql to create sales_weather_clean , with:
- primary/external key constraints
- indexes on join columns
- standardized data

### 🔹 5. *Advanced analyses*
Run 04_queries.sql to explore:
- sales patterns under specific weather conditions
- average sales by temperature/humidity band
- correlations between weather and order volume
- nested queries, aggregations, subqueries

	✏️ See README_queries.md for detailed explanations of queries 11 through 19.

---

## 📈 Expected Results.

- Identification of weather patterns that influence sales.
- Efficient and well-structured database.
- Advanced queries executable in optimal time even on medium to large datasets.

---

## 📬 Contacts

For questions or input:
- GitHub issues
- https://github.com/robertomagno1 -
- https://github.com/JacopoCaldana

---

## 🐣 Happy Easter and happy work!  
Thank you for exploring this project 🌱





