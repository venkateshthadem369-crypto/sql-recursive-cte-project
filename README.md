================================================================
        DATA WAREHOUSE PROJECT - PROJECT SUMMARY
        ETL Pipeline | Bronze Layer | MySQL
        Author: Venkatesh Thadem
================================================================

PROJECT OVERVIEW
----------------
This project implements a full ETL (Extract, Transform, Load) pipeline
that extracts raw data from a source database, applies data quality
transformations, and loads cleaned data into the Bronze Layer of a
data warehouse. The entire pipeline runs via a MySQL stored procedure.

ETL FLOW
--------
datawarehouse (Source) --> Transform & Clean --> bronze (Destination)


SOURCE DATABASE - datawarehouse
--------------------------------
- cust_info       : Raw customer records
- prd_info        : Raw product information
- sales_detals    : Raw sales transactions
- cust_az12       : Additional customer demographics
- loc_a101        : Customer location data
- px_cat_g1v2     : Product category mapping


DESTINATION DATABASE - bronze
------------------------------
- bronze.cust_info       : Cleaned customer data
- bronze.prd_info        : Enriched product data with date ranges
- bronze.sales_details   : Validated sales transactions
- bronze.cust_az12       : Standardized demographics
- bronze.loc_a101        : Normalized location data
- bronze.px_cat_g1v2     : Product category reference


TRANSFORMATIONS APPLIED
------------------------
cust_info     : TRIM names, M/F -> Male/Female, dedup by latest date
prd_info      : Extract category ID, LEAD() for end dates
sales_details : Validate dates, recalculate sales = qty x price
cust_az12     : Strip NAS prefix, future birthdates -> NULL
loc_a101      : Remove dashes, DE->Germany, US/USA->United States
px_cat_g1v2   : Direct load with audit timestamp


STORED PROCEDURE
----------------
Name : silver_layer()
Run  : CALL silver_layer();

Steps inside procedure:
1. TRUNCATE all bronze tables
2. INSERT transformed data from each source table

KEY ACHIEVEMENTS
----------------
- Built complete ETL pipeline from scratch in MySQL
- Implemented NULL handling, deduplication, standardization
- Used ROW_NUMBER(), LEAD(), CASE, COALESCE, NULLIF
- Encapsulated logic in a reusable stored procedure
- Version controlled with Git and GitHub
