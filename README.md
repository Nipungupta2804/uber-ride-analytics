# 🚖 Uber Ride Analytics Pipeline & Dashboard

## 📊 Project Overview
This project builds an end-to-end data analytics pipeline for a ride-hailing platform using Python, SQL, and Power BI.  
It covers data cleaning, transformation, feature engineering, and business analysis to generate actionable insights.

---

## 🎯 Objectives
- Clean and preprocess raw ride data using Python (Pandas)
- Load and transform data in SQL for structured analysis
- Perform feature engineering for business metrics
- Analyze revenue, demand, customers, and driver performance
- Build an interactive Power BI dashboard for decision-making

---

## 🛠️ Tech Stack
- **Python (Pandas)** – Data cleaning ,Preprocessing  
- **SQL (MySQL)** – Data transformation, feature engineering & analysis  
- **Power BI** – Data visualization & dashboarding  

---

## 🔄 Project Workflow

### 1️⃣ Data Cleaning (Python - Pandas)
- Removed duplicates and handled missing values  
- Standardized data formats (timestamps, numerical values)  
- Filtered invalid or inconsistent records  
- Prepared structured dataset for database loading  

---

### 2️⃣ Data Loading (SQL)
- Imported cleaned data into MySQL database  
- Created structured tables (rides, customers, drivers, vehicles)  
- Established relationships using primary and foreign keys  

---

### 3️⃣ Data Cleaning & Transformation (SQL)
- Performed NULL value handling and data corrections  
- Removed duplicates using window functions  
- Applied business rules (e.g., revenue adjustments for cancelled rides)  

---

### 4️⃣ Feature Engineering (SQL)
- Created time-based features (Month, Day, Hour)  
- Calculated delay metrics (Actual vs Estimated time)  
- Categorized rides (Short / Medium / Long)  
- Derived
