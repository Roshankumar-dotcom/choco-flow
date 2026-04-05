![Databricks](https://img.shields.io/badge/Databricks-Data%20Engineering-red)
![Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-orange)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)

# 📊 Chocolate Sales Data Pipeline (Medallion Architecture – Databricks)

## 🚀 Project Overview

This project implements a **Spark Declarative Pipeline (SDP)** using **Databricks** following the **Medallion Architecture (Bronze → Silver → Gold)** pattern.

The objective of this project is to ingest raw sales data from Kaggle, transform and clean it using PySpark, and generate business-ready aggregated datasets for analytics and reporting.

### This project demonstrates:

- End-to-end Data Engineering workflow  
- Data ingestion from external source  
- Data cleaning & transformation using PySpark  
- Layered architecture implementation  
- Business-driven aggregations  
- Production-style pipeline design  

---

## 🏗️ Architecture Overview

The project follows the **Medallion Architecture**:

### 🔹 Bronze Layer (Raw Data)

- Raw data ingested from Kaggle  
- Stored in Databricks (DBFS / Volumes)  
- No major transformations  
- Schema enforcement applied  

### 🔹 Silver Layer (Cleaned & Transformed Data)

- Data type casting  
- Standardized categorical columns (e.g., gender)  
- Date formatting & enrichment  
- Null handling  
- Derived columns (e.g., `membership_days`)  
- Deduplication  
- Data validation  

### 🔹 Gold Layer (Business Aggregations)

- Sales by country  
- Revenue by category  
- Monthly sales trends  
- Customer-level KPIs  
- Profit analysis  
- Top-performing products  

---

## 🛠️ Tech Stack

- Databricks  
- Apache Spark (PySpark)  
- Spark Declarative Pipelines (SDP)  
- Delta Lake  
- Python  
- Kaggle Dataset  
- DBFS  

---

## 📥 Data Source

**Dataset Source:** Kaggle Chocolate Sales Dataset  

### The dataset includes:

- Orders  
- Customers  
- Products  
- Calendar  
- Country  
- Cost & Revenue fields  

---

## 🔄 Data Pipeline Flow

1. Data extracted from Kaggle  
2. Loaded into Databricks Volume (Bronze Layer)  
3. Cleaned & standardized in Silver Layer  
4. Business aggregations built in Gold Layer  
5. Output ready for BI tools / reporting  
