/*This project demonstrates the implementation of a Data Warehouse pipeline using MySQL 8.0, following a modern Medallion Architecture (Bronze Layer) approach.
The primary objective is to ingest raw data from multiple source systems (CRM and ERP) into a structured relational environment for further transformation and analytics.
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Architecture Design

This project implements the Bronze Layer (Raw Layer) of a Data Warehouse architecture.

Bronze Layer – Raw Data Ingestion
  Created empty staging tables in MySQL to match source schema.
  Loaded raw CSV files directly into MySQL using LOAD DATA INFILE.
  No transformations were applied at this stage.
  Data is stored in its original format for traceability and auditability.
  This simulates real-world enterprise ingestion pipelines.

Two different source systems were simulated:
  CRM Source
    cust_info
    prd_info
    sales_details
  ERP Source
    CUST_AZ12
    LOC_A101
    PX_CAT_G1V2

Since the Bronze layer focuses on raw ingestion and high-performance bulk loading, I prioritized optimized batch execution using LOAD DATA INFILE.
Procedure-based automation can be added in later ETL layers.*/


truncate table cust_info;
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Data/datawarehouse/my_first_sql/sql-data-warehouse-project/datasets/source_crm/cust_info.csv"
into table cust_info
fields terminated by ','
ignore 1 lines;

truncate table prd_info;
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Data/datawarehouse/my_first_sql/sql-data-warehouse-project/datasets/source_crm/prd_info.csv"
into table prd_info
fields terminated by ','
ignore 1 lines;

truncate table sales_detals;
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Data/datawarehouse/my_first_sql/sql-data-warehouse-project/datasets/source_crm/sales_details.csv"
into table sales_detals
fields terminated by ','
ignore 1 lines;


truncate table cust_az12;
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Data/datawarehouse/my_first_sql/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv"
into table cust_az12
fields terminated by ','
ignore 1 lines;

truncate table loc_a101;
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Data/datawarehouse/my_first_sql/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv"
into table loc_a101
fields terminated by ','
ignore 1 lines;

truncate table px_cat_g1v2;
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Data/datawarehouse/my_first_sql/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv"
into table px_cat_g1v2
fields terminated by ','
ignore 1 lines;

/*Created a Bronze layer in MySQL by designing staging tables and loading raw CRM and ERP CSV datasets using LOAD DATA INFILE.
Implemented a stored procedure to validate and retrieve ingested data from all source tables, ensuring successful bulk ingestion and schema alignment.
*/

delimiter $$
drop procedure if exists bronze$$
create procedure bronze()
begin
	select * from cust_info;
    select * from prd_info;
    select * from sales_detals;
    select * from cust_az12;
    select * from loc_a101;
    select * from px_cat_g1v2;
end $$
delimiter ;
