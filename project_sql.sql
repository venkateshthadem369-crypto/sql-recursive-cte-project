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