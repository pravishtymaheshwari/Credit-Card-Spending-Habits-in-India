
/*  

Dataset :- Credit Card Spending Habits in India

Description:-
The dataset: Credit Card Spending Habits in India, is taken from Kaggle to perform data exploration. It contains data of expenditures done using 4 different types
of credit cards in 986 distinct cities by Males and Females in 6 different categories; a total of 26052 transactions were done in 2 years. 

Skills used: Joins, CTE's, Window Functions, Window Functions with Aggregate Functions, Converting Data Types, Date Functions,Case Statements,Aggregate Functions

*/



--Top 5 cities with highest spends and their percentage contribution of total credit card spends 

with cte as 
(select city, sum(amount) as totalexpenseofcity from credit_card_transcations$ group by city) 
,cte1 as (select sum(amount) as totalexpense from credit_card_transcations$)
select top 5 cte.*,round((totalexpenseofcity/totalexpense) * 100,2) as contribution_percent from cte1,cte order by totalexpenseofcity desc;


--Highest spend month and amount spent in that month for each card type

with cte as 
(select  card_type, month(transaction_date) as month_, sum(amount) as monthlyexpense from credit_card_transcations$ 
group by card_type,month(transaction_date))
,cte1 as(select *, rank() over(partition by card_type order by monthlyexpense desc) as rank_ from cte)
select * from cte1 where rank_ = 1;


--Transaction details for each card type when it reaches a cumulative of 1000000 total spends

with cte as (
select *,sum(amount) over(partition by card_type order by transaction_date,transaction_id) as total_spend
from credit_card_transcations$)
,cte1 as (select *, rank()over(partition by card_type order by total_spend) as rn from cte where total_spend >=1000000)
select * from cte1 where rn=1;


--City which had lowest percentage spend for gold card type

with cte as (
select city,card_type,sum(amount) as total ,sum(case when card_type = 'Gold' then amount end ) as goldtotal 
from credit_card_transcations$ group by city,card_type )
select top 1 city, sum(goldtotal)/sum(total) as ration from cte 
group by city
having sum(goldtotal)>0
order by ration asc;


--All cities with highest_expense_type and lowest_expense_type columns 

with cte as (
select city,exp_type,sum(amount) as total from credit_card_transcations$ group by city,exp_type)
,cte1 as (select *, dense_rank() over (partition by city order by total desc) as highh, dense_rank()over(partition by city order by total) as minn from cte)
select city,max(case when highh=1 then exp_type end) as highest_expense_type,max(case when minn=1 then exp_type end) as lowest_expense_type
from cte1
group by city;
        
		
--Percentage contribution of spends by females for each expense type

with cte as(
select exp_type,sum(amount) as totalamount,sum(case when gender='F' then amount end) as totalfemaleexpense from credit_card_transcations$ group by exp_type)
select exp_type,round((totalfemaleexpense/totalamount) * 100,2) as percent_contribution from cte order by percent_contribution desc;


--Card and expense type combination which saw highest month over month growth in Jan-2014

with cte as(
select card_type, exp_type,datepart(year,transaction_date) as yr,datepart(month,transaction_date) as mo,sum(amount) as totalsales from credit_card_transcations$ group by card_type,exp_type,datepart(year,transaction_date),datepart(month,transaction_date))
,cte1 as (select *,lag(totalsales) over(partition by card_type,exp_type order by yr,mo asc) as previousmonthsale from cte  )
select top 1 card_type,exp_type,totalsales-previousmonthsale as mom from cte1
where yr = '2014' and mo='01'
order by mom desc;


--City which had highest total spend to total no. of transcations ratio during weekends 

with cte as (
select city, count(transaction_id) as transactions,sum(amount) as totalspend from credit_card_transcations$
where datepart(weekday,transaction_date) = 1 or datepart(weekday,transaction_date) = 7
group by city)
select top 1 city,totalspend/transactions as ratio from cte order by ratio desc;


--City which took least number of days to reach its 500th transaction after the first transaction in that city

with cte as(
select *,row_number()over(partition by city order by transaction_date,transaction_id) as rn from credit_card_transcations$)
select top 1 city,datediff(day,min(transaction_date),max(transaction_date)) as duration from cte
where rn=1 or rn=500
group by city
having count(1) = 2
order by duration;

