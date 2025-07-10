# ⭐️ Data Modeling - Data Lakehouse (Star Schema)

This repository contains an example implementation of a **Star Schema** data model designed for a Data Lakehouse architecture.

## 📌 Project Goal

To demonstrate how to model a classic **Star Schema** for analytics use cases in a Data Lakehouse or Data Warehouse environment.

---

## 📂 Repository Structure


- **star_schema.sql** — contains SQL DDL statements to create dimension and fact tables for a Star Schema.

---

## 🗺️ Schema Overview

This Star Schema model includes:

✅ DimCustomer  
✅ DimEmployee  
✅ DimProduct  
✅ DimDate  
✅ FactSales  

✔️ **FactSales** is the central fact table storing sales transactions, linked to dimension tables via foreign keys.

---

## 🎯 Tables and Relationships

- **DimCustomer:** Stores customer attributes.
- **DimEmployee:** Stores employee attributes.
- **DimProduct:** Stores product attributes.
- **DimDate:** Stores date dimension for time-based analytics.
- **FactSales:** Stores transaction-level sales data with foreign keys to dimension tables.

**FactSales** references:  
- CustomerKey → DimCustomer  
- EmployeeKey → DimEmployee  
- ProductKey → DimProduct  
- DateKey → DimDate

---

## 📑 Example Use Cases

- Building business intelligence dashboards
- Analyzing sales by customer, product, employee
- Time-series sales reporting

---

## 🛠️ How to Use

1️⃣ Clone this repository:  

2️⃣ Open `star_schema/star_schema.sql` in your SQL editor.

3️⃣ Run the DDL statements in your preferred database (e.g., SQL Server, PostgreSQL, MySQL).

---

## 💡 Future Improvements

- Add INSERT sample data
- Provide ERD / schema diagram (visual)
- Add stored procedures for ETL loading
- Include example Power BI or dashboard queries

---

## 🙌 Author

Giorgi Megeneishvili  
[LinkedIn](https://www.linkedin.com/in/giorgi-megeneishvili-313b13248)

---

## 📜 License

This project is open-source. Feel free to use and modify as needed.
