use RFM_analysis

-- OVERVIEW THE DATASET

select * from online_retail
order by InvoiceDate

-- DATA CLEANING
	-- Removing Missing Values - Duplicates, Converting features to the correct Data type, Calculating TotalPrice column

Drop table if exists online_retail_update
with duplicate_check as
	(select * , ROW_NUMBER() over (partition by InvoiceNo, StockCode, Quantity, InvoiceDate,
	CustomerID order by InvoiceDate) as duplicate_check
	from online_retail)
select InvoiceNo, StockCode, Description, Quantity, InvoiceDate, 
UnitPrice, CustomerID, Country, (quantity * UnitPrice) as TotalPrice into online_retail_update from duplicate_check
where InvoiceNo IS NOT NULL and Quantity > 0 and duplicate_check = 1 and CustomerID IS NOT NULL

alter table online_retail_update
alter column InvoiceDate date

select * from online_retail_update
order by InvoiceNo


-- RFM ANALYSIS --

-- Step 1: Compute for recency, frequency, and monetary values per customer
drop table if exists rfm_segment_table
With
-- reference date ( I chose a reference date, which is the day after the last purchase date in the dataset.)
time1 as (
	select DATEADD(day,1,max(InvoiceDate)) as reference_date from online_retail_update),
--Compute for F & M
t1 as (
    select  
    CustomerID,
    Country,
    MAX(InvoiceDate) as last_purchase_date,
    COUNT(distinct InvoiceNo) as frequency,
    SUM(TotalPrice) as monetary 
    from online_retail_update
    group by CustomerID, Country), 
--Compute for R
t2 as (
	select CustomerID, Country,last_purchase_date,  
	datediff(day, last_purchase_date,reference_date) as recency,frequency, monetary
	from t1,time1),
	
-- Step 2: Determine quintiles for each RFM metric
/* The next step would be to group the customers into quintiles in terms of their RFM values — we divide the customers into 5 equal groups */

--Determine quintiles for RFM
 
t3 as(
	select *,
	round(percent_rank() over (order by recency DESC)*100,2) as per_recency,
	round(percent_rank() over (order by frequency)*100,2) as per_frequency,
	round(percent_rank() over (order by monetary)*100,2) as per_monetary
	from t2),

--Assign scores for R - F - M
t4 as (
	select *,
	-- Recency
	case when per_recency >= 0 AND per_recency <= 20 then 1
		when per_recency > 20 AND per_recency <= 40 then 2
		when per_recency > 40 AND per_recency <= 60 then 3
		when per_recency > 60 AND per_recency <= 80 then 4
		when per_recency > 80 AND per_recency <= 100 then 5
	end as Recency_rank,
	-- Frequency
	case when per_frequency >= 0 AND per_frequency <= 20 then 1
		when per_frequency > 20 AND per_frequency <= 40 then 2
		when per_frequency > 40 AND per_frequency <= 60 then 3
		when per_frequency > 60 AND per_frequency <= 80 then 4
		when per_frequency > 80 AND per_frequency <= 100 then 5
	end as Frequency_rank,
	-- Monetary
	case when per_monetary >= 0 AND per_monetary <= 20 then 1
		when per_monetary > 20 AND per_monetary <= 40 then 2
		when per_monetary > 40 AND per_monetary <= 60 then 3
		when per_monetary > 60 AND per_monetary <= 80 then 4
		when per_monetary > 80 AND per_monetary <= 100 then 5
	end as Monetary_rank
	from t3),

-- Step 3: Define the RFM segments using the RFM scores
t5 as (
	select *,
	concat(Recency_rank, Frequency_rank , Monetary_rank) AS rfm_score
	from t4),
t6 as (
	select *,
	case
		when rfm_score in ('555','554','544','545','454','455','445') then 'Champions'
		when rfm_score in ('543','444','435','355','354','345','344','335') then 'Loyal Customers'
		when rfm_score in ('553','551','552','541','542','533','532','531','452','451','442','441','431','453','433','432','423','353','352','351','342','341','333','323')then 'Potential Loyalist'
		when rfm_score in ('512', '511', '422', '421', '412', '411', '311') then 'New Customer'
		when rfm_score in ('525', '524', '523', '522', '521', '515', '514', '513', '425', '424', '413', '414', '415', '315', '314', '313') then 'Promising'
		when rfm_score in ('535', '534', '443', '434', '343', '334', '325', '324') then 'Need Attention'
		when rfm_score in ('155', '154', '144', '214','215','115', '114', '113') then 'Cannot Lose Them'
		when rfm_score in ('331', '321', '312', '221', '213') then 'About To Sleep'
		when rfm_score in ('255', '254', '245', '244', '253', '252', '243', '242', '235', '234', '225', '224', '153', '152', '145', '143', '142', '135', '134', '133', '125', '124') then 'At Risk'
		when rfm_score in ('332', '322', '231', '241', '251', '233', '232', '223', '222', '132', '123', '122', '212', '211') then 'Hibernating'
		when rfm_score in ('111', '112', '121', '131', '141', '151') then 'Lost'
	end as rfm_segment
	from t5),
t7 as (
	select o.*, r.recency, r.frequency, r.monetary, r.rfm_segment from online_retail_update o
	left join  t6 r
	on o.CustomerID = r.CustomerID)
select * into rfm_segment_table from t7

select * from rfm_segment_table

-- COHORT ANALYSIS --
--Initial Start Date (First Invoice Date)
drop table if exists cohort_rentention
with
first_purchase1 as(
	Select distinct c1.date, 
	datediff( MONTH, c1.first_purchase_date, c1.date) AS month_order,
	format(c1.first_purchase_date, 'yyyy MM') AS first_purchase,
	c1.TotalPrice, c1.CustomerID
	from (
		select CustomerID,InvoiceDate as date,
		first_value(InvoiceDate) over (partition by CustomerID order by Invoicedate) AS first_purchase_date,
		TotalPrice
		from online_retail_update) c1),
c2 as (
	select  
    first_purchase,
    month_order,
    round(sum(TotalPrice),2) AS TotalSales,
	round(avg(TotalPrice),2) AS AvgSales,
    count(distinct CustomerID) AS Customers
    from first_purchase1
    group by first_purchase, month_order),
c_cohort as (
     select a.*,
     a.TotalSales/a.CohortTotalSales AS CohortSalesPerc,
     cast(a.Customers as float)/cast(a.CohortCustomers as float ) AS CohortCustomersPerc
     from (
         select *,
		first_value(TotalSales) over(partition by first_purchase order by month_order) AS CohortTotalSales,
        first_value(AvgSales) over(partition by first_purchase order by month_order) AS CohortAvgSales,
		first_value(Customers) over(partition by first_purchase order by month_order) AS CohortCustomers
		from c2) a)
select * into cohort_rentention from c_cohort

select * from cohort_rentention
order by first_purchase, month_order

-- REPEAT PURCHASE ANALYSIS --
drop table if exists repeat_purchase
with t_repeat as (
   select *,
   case when a.customer_seq > 1 then 'Repeat Customer'
   else 'New Customer'
   end as RepeatPurchase 
   from ( 
      select  *,
      dense_rank() over (partition by CustomerID order by InvoiceDate) as customer_seq
      FROM online_retail_update) a),

 t_repurchase as (
    select *, 
    datediff(MONTH, b.first_purchase, b.InvoiceDate) as month_order
    from (
        select *,
        first_value(InvoiceDate) over (partition by CustomerID order by InvoiceDate) as first_purchase
        FROM t_repeat) b),

  t_previous_purchase as (
    select *, 
    datediff(DAY, c.InvoiceDate,c.next_purchase) as days_bet_purchase
    FROM (
        select *,
        lead(InvoiceDate) over (partition by CustomerID order by InvoiceDate) as next_purchase
        from t_repurchase) c)

select * into repeat_purchase from t_previous_purchase

select * from repeat_purchase
order by CustomerID, customer_seq
