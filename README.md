# 🍫 Chocolate Sales Analytics (2023 – 2024)

<p align="center">

<img src="https://upload.wikimedia.org/wikipedia/commons/f/f3/Apache_Spark_logo.svg" width="120"/>

</p>

<p align="center">

<img src="https://img.shields.io/badge/Python-3.x-3776AB?logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/Apache%20Spark-Big%20Data-FDEE21?logo=apachespark&logoColor=black"/>
<img src="https://img.shields.io/badge/PySpark-ETL-FF9900?logo=apache&logoColor=white"/>
<img src="https://img.shields.io/badge/Databricks-FF3621?logo=databricks&logoColor=white"/>
<img src="https://img.shields.io/badge/SQL-Analytics-336791?logo=postgresql&logoColor=white"/>
<img src="https://img.shields.io/badge/Kaggle-Dataset-20BEFF?logo=kaggle&logoColor=white"/>

</p>

---

## 📌 Project Overview

This project analyzes a Chocolate Sales Dataset (2023–2024) using Python, Apache Spark, SQL, and Databricks.

It demonstrates an end-to-end **Data Engineering + Analytics workflow**, including:

- Data ingestion from Kaggle
- Structured data transformation
- SQL-based exploration
- Implementation of Medallion Architecture
- Preparing curated datasets for analytics & reporting

This project is designed as a portfolio project to showcase real-world Data Engineer / Data Analyst skills.

---

# 🏗️ Architecture Overview

This project follows the **Medallion Architecture** pattern commonly used in modern data platforms.

## 🥉 Bronze Layer – Raw Data

- Data is downloaded from Kaggle using API.
- Raw CSV files are stored without modification.
- Files include:
  - calendar.csv
  - customers.csv
  - products.csv
  - sales.csv
  - stores.csv

Purpose:
- Preserve original source data
- Enable reproducibility
- Maintain raw audit layer

---

## 🥈 Silver Layer – Cleaned & Transformed Data

- Data is cleaned and validated.
- Schema corrections applied.
- Missing values handled.
- Data types standardized.
- Basic joins and enrichment performed.

Purpose:
- Create structured and reliable datasets.
- Remove inconsistencies from raw data.
- Prepare data for analytics consumption.

---

## 🥇 Gold Layer – Business-Ready Data

- Aggregated tables created.
- Business KPIs calculated.
- Metrics such as:
  - Total Sales
  - Revenue Trends
  - Product Performance
  - Store Performance
  - Customer Segmentation

Purpose:
- Deliver analytics-ready datasets.
- Enable dashboarding and reporting.
- Support business decision-making.

---

# 📁 Project Structure

```
Chocolate Sales Dataset 2023 - 2024/
│
├── chocolate_sales_sdp/
│   ├── explorations/
│   ├── transformations/
│   ├── utilities/
│   └── README.md
│
├── Choclate_sales _Data_Explorations.ipynb
├── Getting_Chocolate_Sales_Dataset_Kaggle.ipynb
```

---

# 📦 chocolate_sales_sdp Folder Explanation

This folder contains the **core pipeline logic** of the project.

It follows modular and scalable data engineering practices.

---

## 📂 explorations/

- Used for exploratory analysis.
- Ad-hoc queries and data inspection.
- Used before formalizing transformations.

Demonstrates:
- Data profiling
- Understanding schema
- Business question exploration

---

## 📂 transformations/

This is the **core ETL layer** of the project.

Contains:
- Data loading scripts
- Cleaning logic
- Transformation rules
- Aggregation logic
- Dataset definitions

Each dataset transformation is modular and reusable.

This represents:
- Silver Layer transformations
- Gold Layer aggregations

---

## 📂 utilities/

Contains helper functions such as:
- Reusable data loading methods
- Logging utilities
- Common transformation helpers

Helps:
- Avoid code duplication
- Improve maintainability
- Follow clean code practices

---

# 📘 Notebooks Overview

## 1️⃣ Getting_Chocolate_Sales_Dataset_Kaggle.ipynb

Responsible for:

- Kaggle API configuration
- Dataset download
- Extracting raw files
- Loading into Spark DataFrames
- Schema verification

Skills Demonstrated:
- API integration
- Data ingestion
- Big data handling using Spark

---

## 2️⃣ Choclate_sales _Data_Explorations.ipynb

Responsible for:

- SQL queries
- Table inspection
- Initial business insights
- Data validation

Skills Demonstrated:
- SQL proficiency
- Analytical thinking
- Data quality checks

---

# 🛠️ Technologies Used

- Python
- Apache Spark
- SQL
- Databricks Spark Declarative Pipeline
- Kaggle API
- Databricks Notebook

---

# 🎯 Skills Demonstrated

✔ End-to-End Data Pipeline Development  
✔ Medallion Architecture Implementation  
✔ Data Engineering Best Practices  
✔ SQL-Based Analytics  
✔ Big Data Processing  
✔ Modular Project Structure  
✔ Production-Style Data Transformations  

---

# 💼 Recruiter Summary

This project demonstrates my ability to:

- Work with real-world datasets
- Design scalable data architectures
- Implement Medallion data modeling
- Build structured transformation pipelines
- Deliver analytics-ready datasets

I am actively seeking opportunities in:

- Data Engineer (Entry-Level)
- Data Analyst
- Business Intelligence Developer
- Analytics Engineer

---

# 📬 Contact

If you would like to discuss this project or collaborate, feel free to connect.

---

⭐ If you found this project helpful, consider giving it a star!
