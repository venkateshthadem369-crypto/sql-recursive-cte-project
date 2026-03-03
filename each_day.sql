with recursive cte as
(
	select distinct submission_date, hacker_id
	from my_csv
    where submission_date = (select min(submission_date) from my_csv)
    union
    select s.submission_date, s.hacker_id
    from my_csv s
    join cte 
		on cte.hacker_id = s.hacker_id
	where s.submission_date = (
								select min(submission_date)
                                from my_csv
                                where submission_date > cte.submission_date
							  )
),

no_of_unique_hackers as
(
	select submission_date, count(*) as no_of_unique_hackers
	from cte
	group by submission_date
	order by 1
),
no_submissions as
(
	select submission_date, hacker_id, count(*) no_of_submissions
    from my_csv
    group by submission_date, hacker_id
    order by 1
),

	max_no_submissions as
    (
		select submission_date, max(no_of_submissions) max_no_of_submissions
        from no_submissions
        group by submission_date
        order by 1
    ),
    
    final as
    (
		select ns.submission_date,min(ns.hacker_id) final_hacker
		from max_no_submissions ms
		join no_submissions ns
			on ms.submission_date = ns.submission_date and ms.max_no_of_submissions = ns.no_of_submissions
		group by ns.submission_date
    )
select f.submission_date,f.final_hacker,m.max_no_of_submissions,h.name
from max_no_submissions m
join final f
	on m.submission_date = f.submission_date
join hackers h 
	on h.hacker_id = f.final_hacker