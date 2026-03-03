select *
from
(
	select * ,row_number() over(partition by cst_id order by cst_create_date desc) as rn
	from datawarehouse.cust_info
)t
where rn=1 and cst_id != 0
order by rn
