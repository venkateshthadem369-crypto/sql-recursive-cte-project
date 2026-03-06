delimiter $$
drop procedure if exists silver_layer $$
create procedure silver_layer()
Begin
	/* we can use belw syntax for creating empty tables like another one*/
	-- CREATE TABLE bronze.sales_details
	-- LIKE datawarehouse.sales_detals;
	
	insert into bronze.cust_info
	select * 
	from 
	(
		select 
		cst_id,
	    cst_key,
	    trim(cst_firstname),
	    trim(cst_lastname),
	    (
			case upper(cst_marital_status)
				when 'M' then 'Male'
				when 'F' then 'Female'
				else 'other'
			end
	    ) cst_marital_status,
	    (
			case upper(cst_gndr)
				when 'M' then 'Male'
				when 'F' then 'Female'
				else 'other'
			end
	    ) cst_gndr,
	    cst_create_date,
	    current_date() as dwh_create_date
		from
		(
			select *
			from
			(
				select * ,row_number() over(partition by cst_id order by cst_create_date desc) as rn
				from datawarehouse.cust_info
			)t
			where rn=1 and cst_id != 0
			order by rn
		)temp
	) temp;
	
	insert into bronze.prd_info
	select *
	from
	(
		select 
			prd_id,
			prd_key,
			replace(substring(prd_key,1,5),'-','_') cat_id,
			substring(prd_key,7,length(prd_key)) prd_key1,
			coalesce(prd_cost,0) prd_cost,
			case trim(prd_line)
				when 'R' then 'Road'
				when 'S' then 'other_sales'
				when 'M' then 'Mountain'
				when 'T' then 'Touring'
				else 'Not_available'
			end prd_line,
			prd_start_dt,
			lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-interval 1 day prd_end_dt,
	        current_date() dwh_create_dt
		from datawarehouse.prd_info
	)temp;
	
	-- CREATE TABLE bronze.sales_details
	-- LIKE datawarehouse.sales_detals;
	
	-- alter table sales_details 
	-- add column dwh_create_dt datetime default current_timestamp;
	
	select 
		sls_ord_num,
	    sls_prd_key,
	    sls_cust_id,
	    case 
			when sls_order_dt =0 or length(sls_order_dt) != 10 then null
	        else sls_order_dt
		end sls_order_dt,
	    case 
			when sls_ship_dt = 0 or length(sls_ship_dt) != 10 then null
	        else sls_ship_dt
		end sls_ship_dt,
	    case 
			when sls_due_dt = 0 or length(sls_due_dt) != 10 then null
	        else sls_due_dt
		end sls_due_dt,
	    sls_sales,
	    sls_quantity,
	    sls_price
	from datawarehouse.sales_detals;
	
	-- CREATE TABLE bronze.sales_details
	-- LIKE datawarehouse.sales_detals;
	
	-- alter table sales_details 
	-- add column dwh_create_dt datetime default current_timestamp;
	
	insert into bronze.sales_details
	select sls_ord_num,
		   sls_prd_key,
	       sls_cust_id,
	       sls_order_dt,
	       sls_ship_dt,
	       sls_due_dt,
			case
					when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * abs(sls_price)
					else sls_sales
				end sls_sales,
			sls_quantity,
	        sls_price,
	        current_timestamp() as dwh_create_dt
	from
	(
		select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case 
				when sls_order_dt =0 or length(sls_order_dt) != 10 then null
				else sls_order_dt
			end sls_order_dt,
			case 
				when sls_ship_dt = 0 or length(sls_ship_dt) != 10 then null
				else sls_ship_dt
			end sls_ship_dt,
			case 
				when sls_due_dt = 0 or length(sls_due_dt) != 10 then null
				else sls_due_dt
			end sls_due_dt,
			sls_sales,
			sls_quantity,
			CASE
						WHEN sls_price IS NULL
						  OR sls_price <= 0
						THEN CAST(ABS(sls_sales) / NULLIF(sls_quantity, 0) AS SIGNED)
						ELSE sls_price
					END AS sls_price
		from datawarehouse.sales_detals
	)t;
	
	-- create table bronze.cust_az12
	-- like datawarehouse.cust_az12
	
	-- alter table bronze.cust_az12
	-- add column dwh_create_dt datetime default current_timestamp
	
	insert into bronze.cust_az12
	SELECT
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
			ELSE cid
		END AS cid,
		CASE
			WHEN bdate > CURDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE
			WHEN UPPER(TRIM(REPLACE(gen, '\r', ''))) IN ('M', 'MALE')   THEN 'Male'
			WHEN UPPER(TRIM(REPLACE(gen, '\r', ''))) IN ('F', 'FEMALE') THEN 'Female'
			ELSE 'na'
		END AS gen,
	    current_timestamp() as dwh_create_dt
	FROM datawarehouse.cust_az12;
	
	-- create table bronze.loc_a101
	-- like datawarehouse.loc_a101;
	
	-- alter table bronze.loc_a101
	-- add column dwh_create_dt datetime default current_timestamp 
	
	insert into bronze.loc_a101
	select replace(cid,'-','') cid,
		case 
			when trim(replace(cntry,'\r','')) = 'DE' then 'Germany'
			when trim(replace(cntry,'\r','')) in ('US','USA') then 'United States'
			when trim(replace(cntry,'\r','')) is null or trim(replace(cntry,'\r','')) = '' then 'not_available'
			else trim(replace(cntry,'\r',''))
		end cntry,
	    current_timestamp() dwh_create_dt
	from datawarehouse.loc_a101;
	
	-- create table bronze.px_cat_g1v2
	-- like datawarehouse.px_cat_g1v2
	
	-- alter table px_cat_g1v2
	-- add column dwh_create_dt datetime default current_timestamp
	
	insert into bronze.px_cat_g1v2
	select ID,
		cat,
	    subcat,
	    MAINTENANCE,
	    current_timestamp() as dwh_create_dt
	from datawarehouse.px_cat_g1v2;
End $$
delimiter ;
