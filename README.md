**\# SQL Recursive CTE Analysis**



This project demonstrates the use of \*\*Recursive Common Table Expressions (CTE)\*\* in SQL to analyze hacker submissions.



\## Problem

Identify hackers who submitted solutions continuously across challenge days and determine the hacker with the highest submissions per day.



\## Concepts Used

\- Recursive CTE

\- Aggregations

\- Joins

\- Window-like logic

\- SQL Query Optimization



**\## Dataset**

Submission dataset containing:

\- submission\_date

\- hacker\_id

\- number\_of\_submissions



\## Files

queries/recursive\_cte\_solution.sql – main SQL solution

data/my\_csv\_sample.csv – sample dataset



Example Query Snippet

WITH RECURSIVE cte AS

(

&#x20;   SELECT DISTINCT submission\_date, hacker\_id

&#x20;   FROM submissions

&#x20;   WHERE submission\_date = (SELECT MIN(submission\_date) FROM submissions)



&#x20;   UNION



&#x20;   SELECT s.submission\_date, s.hacker\_id

&#x20;   FROM submissions s

&#x20;   JOIN cte

&#x20;       ON cte.hacker\_id = s.hacker\_id

&#x20;   WHERE s.submission\_date =

&#x20;   (

&#x20;       SELECT MIN(submission\_date)

&#x20;       FROM submissions

&#x20;       WHERE submission\_date > cte.submission\_date

&#x20;   )

)

SELECT \*

FROM cte;

**Project Structure**

sql-recursive-cte-analysis

│

├── data

│   └── submissions\_sample.csv

│

├── queries

│   └── recursive\_cte\_solution.sql

│

├── docs

│   └── explanation.md

│

└── README.md



\## Author

Venkatesh Thadem

B.Tech IT | SQL | Python | Machine Learning

