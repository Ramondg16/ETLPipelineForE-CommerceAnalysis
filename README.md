# ETL Pipeline for E-Commerce Analysis and Retention

This project implements a production-ready Extract, Transform, Load (ETL) pipeline designed for Customer Churn Analysis. It integrates transactional e-commerce data with macro-economic indicators to provide a comprehensive view of customer retention.

## Project Overview
In this project, I developed a modular pipeline to process raw retail data, perform advanced feature engineering in Python, and load the results into a structured MySQL Star Schema optimized for analytical querying.

### Key Features
* **Multi-Source Integration**: Combines Online Retail and behavioral data with Personal Consumption Expenditures (PCE) for economic context.
* **Automated Feature Engineering**: Python-driven logic for calculating Return Flags, Customer Age Segmentation, and Purchase Frequency.
* **Star Schema Optimization**: A robust database design featuring a central Fact table and descriptive Dimension tables for Customers, Products, and the Economy.
* **Scalable Loading**: Implements batch insertion techniques to ensure data integrity and performance.

## Technical Stack
* **Language**: Python 3.x (using Pandas and SQLAlchemy).
* **Database**: MySQL.
* **Environment**: Modular project structure with .env configuration for secure credential management.

## Repository Structure
* **/scripts**: Contains Python ETL logic for extraction and transformation.
* **/schema**: SQL scripts for database initialization and Star Schema creation.
* **/config**: Configuration templates for database connectivity.
* **/utils**: Helper functions for logging and data validation.

## Business Insights
The primary goal of this pipeline is to answer critical business questions regarding:
1. Identifying customer segments with the highest churn rates.
2. Analyzing correlations between macro-economic trends (PCE) and product return rates.
3. Determining which product categories drive the most long-term retention.
