### Data Warehouse and Analytics Project
Building a modern data warehouse with SQL server, including ETL processes, data modeling and analytics 

Welcome to the ** Sql Data Warehouse and Analytics Project **
This project demonstrates a comprehensive data warehousing and analytics solution, from building a data warehouse to generating actionable insights. Designed as a portfolio project highlights industry best practices in data engineering and analytics 


###  Data Architecture 

The data architecture follows the Medallion architecture Bronze, Silver, Gold

    
•	1.Bronze Layer : Stores raw data  as is from the source systems. Data is ingested from CSV files into SQL server Database .Since SQL Server could not be installed directly on my local machine, I used Docker as a containerized alternative by pulling the official Microsoft SQL Server image and running it as a container. To enable SQL Server access to the raw source data, I mounted the local directory containing the CSV files as a volume inside the Docker container, allowing the database engine to read and load the files directly. This approach allowed me to replicate a local SQL Server environment without a native installation, while keeping the raw CSV files accessible within the container as the entry point for the Bronze layer of the Medallion Architecture
•	2.Silver Layer : This Layer includes data cleansing, standardization , and normalization processess to prepare data for analysis 
•	3.Gold Layer : Houses business ready data modeled into a star schema required for reporting and analysis


---
### Project Requirements
### Building the Data Warehouse (Data Engineering)
**
### Objective 
Develop a modern data warehouse using SQL server to consolidate sales data, enabling analytical reporting and informed decision making.

### Specifications

- **Data Sources** : Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality** : cleanse and resolve data quality issues prior to analysis.
- **Integration**  : Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope** : Focus on the latest dataset only; historization of data is not required.
- **Documenation** : Provide clear documenation of the data model to support both business stakeholders and analytics teams.


----

### BI : Analytics & Reporting (Data Analytics)


### Objective 
Develop SQL-based analytics to deliver detailed insights into :
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**


These insights empower stakeholders with key business metrics, enabling stragetic decision-making 

---

This project is licesned under [MIT License]_(LICENSE)_. You are free to use, modify, share this project with proper attribution.

## About Me 

Hi there! I'm **Srikanth Aldi**. I'm a Data Engineer and passionate about analytics 


