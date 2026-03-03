use datawarehouse;
delimiter $$
drop procedure if exists bronze$$
create procedure bronze()
begin
	select * from cust_info limit 100;
    select * from prd_info limit 100;
    select * from sales_detals limit 100;
    select * from cust_az12 limit 100;
    select * from loc_a101 limit 100;
    select * from px_cat_g1v2 limit 100;
end $$
delimiter ;

