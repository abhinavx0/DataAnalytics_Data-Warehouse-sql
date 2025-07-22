# Data Warehouse and Analytics
Welcome to the **Data Warehouse and Analytics Project** repository!
This project guides you through the end-to-end process of building a modern data warehouse and turning raw data into meaningful insights. It's designed as a hands-on portfolio project based on real-world data engineering and analytics practices.

---
## Data Architechture.
The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
<img width="856" height="600" alt="Image" src="https://github.com/user-attachments/assets/ece01402-0f90-4f58-bbdd-ce732672fb39" />

1. **Bronze Layer**: Stores raw data exactly as received from source CSV files. Data is ingested into the **MySQL database** without transformation.
2. **Silver Layer**: Performs data cleansing, standardization, and normalization to prepare the data for reliable analysis and downstream use.
3. **Gold Layer**: Contains business-ready data organized in a **star schema**, optimized for reporting, dashboards, and advanced analytics.

--- 
## 📖 Project Overview

This project covers the full lifecycle of building a data warehouse and delivering business insights:

1. **Data Architecture**: Designing a modern warehouse using the Medallion Architecture with **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from CSV sources into a structured warehouse.
3. **Data Modeling**: Creating optimized fact and dimension tables for fast and flexible analysis.
4. **Analytics & Reporting**: Writing SQL queries and building dashboards to uncover key business insights.

🎯 This repository is aiming to demonstrate skills in:

* SQL Development
* Data Architecture
* Data Engineering
* ETL Pipeline Design
* Data Modeling
* Data Analytics

Sure! Here's a polished and rewritten version of your project requirements, tailored for clarity and professionalism:

---

## 🚀 Project Requirements

### 🏗️ Data Warehouse Development (Data Engineering)

#### 🎯 Objective

Design and implement a modern data warehouse using **MySQL** to consolidate and organize sales data for analytical reporting and better decision-making.

#### 📋 Key Specifications

* **Data Sources**: Ingest data from two primary sources — **ERP** and **CRM**, both provided as CSV files.
* **Data Cleansing**: Address and fix data quality issues before loading to ensure accuracy and consistency.
* **Data Integration**: Merge ERP and CRM data into a unified, analysis-friendly schema.
* **Project Scope**: Work with the most recent snapshot of data; historical tracking is not required.
* **Documentation**: Provide clear and comprehensive documentation of the data model to support analysts and business users.

---

### 📊 BI & Analytics (Data Analysis)

#### 🎯 Objective

Develop SQL-based insights to support strategic decision-making by analyzing:

* **Customer Patterns & Segmentation**
* **Product-Level Performance**
* **Sales Trends & Seasonal Insights**

The resulting metrics will provide actionable intelligence for stakeholders, supporting both operational and strategic goals.

---
## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

## 🌟 About Me

Hi there! I'm Abhinav Om, currently a 3rd-year undergraduate student at the Indian Institute of Information Technology (IIIT) Ranchi.
I'm passionate about turning raw data into meaningful insights and am actively working toward a career as a Data Analyst or Business Analyst.

I enjoy solving real-world problems through data, exploring trends, and drawing actionable conclusions that drive decision-making.
I'm constantly improving my skills in SQL, Excel, Python, and data visualization tools like Power BI and Tableau.
With hands-on project experience in data warehousing and analytics, I'm building a strong foundation for a future in analytics and consulting.
