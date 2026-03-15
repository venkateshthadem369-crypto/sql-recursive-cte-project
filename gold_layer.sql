drop view if exists gold.dim_customers;
create view gold.dim_customers as 
select 
	row_number() over(order by ci.cust_id) as customer_key,
	ci.cust_id,
	ci.cst_key,
	concat(ci.cst_firstname,' ',ci.cst_lastname) cst_name,
	ci.cst_marital_status,
	case 
		when ci.cst_gndr!='other' then ci.cst_gndr
		else coalesce(ca.gen,'not_available')
	end new_gen,
	ci.cst_create_date,
	ca.BDATE,
	la.cntry
from bronze.cust_info ci
left join bronze.cust_az12 ca
	on ci.cst_key = ca.cid
left join bronze.loc_a101 la
	on ci.cst_key = la.cid;
    
drop view if exists gold.dim_products;
create view gold.dim_products as
select 
-- 	row_number() over(order by pi.prd_start_dt,pi.prd_key) product_key,
	pi.prd_id product_id,
    pi.prd_key product_number,
    pi.prd_num product_name,
    pi.cat_id,
    pcg.cat,
    pcg.subcat,
    pi.prd_cost,
    pi.prd_line,
    pi.prd_start_dt,
    pcg.maintenance
from prd_info pi
left join bronze.px_cat_g1v2 pcg
	on pi.prd_key = pcg.id
 where prd_end_dt is null;
 
 drop view if exists gold.fact_sales;
create view gold.fact_sales as
select
	sd.sls_ord_num,
	sd.sls_prd_key,
	dm.customer_key,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price,
	sd.sls_order_dt,
	sd.sls_ship_dt,
	sd.sls_due_dt
from bronze.sales_details sd
left join gold.dim_customers dm
	on sd.sls_cust_id = dm.cust_id
left join gold.dim_products dp
	on sd.sls_prd_key = dp.product_number