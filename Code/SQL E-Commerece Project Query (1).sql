--Sum of amounts
select c.customer_state,datename(MONTH,o.order_purchase_timestamp) as Month, year(o.order_purchase_timestamp) as Year,SUM(p.payment_value) 
from order_payments as p
inner join orders as o on o.order_id=p.order_id
inner join customers as c on c.customer_id=o.customer_id
where o.order_delivered_customer_date is not null 
group by year(o.order_purchase_timestamp),datename(MONTH,o.order_purchase_timestamp),c.customer_state




--Customer count
Select Datename(year,order_purchase_timestamp) as year,
Datename(MONTH,order_purchase_timestamp) as Month,
customer_state,count(customer_unique_id)as count from
(select o.*,c.customer_state,c.customer_unique_id,dense_rank()over(partition by c.customer_unique_id 
order by o.order_purchase_timestamp) as rank_ from orders as o 
inner join customers as c on c.customer_id=o.customer_id) d where rank_=1 
group by Datename(year,order_purchase_timestamp),Datename(MONTH,order_purchase_timestamp),customer_state


--Order count
select Datename(year,o.order_purchase_timestamp) as year,Datename(MONTH,o.order_purchase_timestamp) as Month,
c.customer_state,count(o.order_id)as count from orders as o 
inner join customers as c on c.customer_id=o.customer_id
group by Datename(year,o.order_purchase_timestamp),Datename(MONTH,o.order_purchase_timestamp),c.customer_state


--Average delivery tiME for various cities
select distinct c.customer_state,datename(month,o.order_delivered_customer_date) as month,
datename(year,o.order_delivered_customer_date) as year,avg(DATEDIFF(DAY,o.order_purchase_timestamp,o.order_delivered_customer_date))
over(partition by c.customer_state,datename(month,o.order_delivered_customer_date),
datename(year,o.order_delivered_customer_date)) AS Average_delivery_time  from orders as o
inner join customers as c on c.customer_id=o.customer_id 
where c.customer_state in ('SP','RJ','AP','RR') and datename(month,o.order_delivered_customer_date) is not null

--Count of sellers for consumers
Select customer_state,customer_city,customer_cOUNT,seller_count from(select distinct customer_state,customer_city,
count( distinct customer_unique_id) as customer_count from customers 
group by customer_state,customer_city) as d
INNER JOIN (select distinct seller_state,seller_city,count(seller_id) as seller_count from sellers group by
seller_state,seller_city) as s on S.seller_state=D.customer_state AND S.seller_city=D.customer_city 
WHERE customer_state IN('SP','RJ','AP','RR') 


--Average lead time delivery
select distinct c.customer_state,datename(month,o.order_delivered_customer_date) as month,
datename(year,o.order_delivered_customer_date) as year,avg(DATEDIFF(DAY,O.order_delivered_customer_date,O.order_estimated_delivery_date))
over(partition by c.customer_state,datename(month,o.order_delivered_customer_date),
datename(year,o.order_delivered_customer_date)) AS Avg_lead_delivery  from orders as o
inner join customers as c on c.customer_id=o.customer_id
where c.customer_state in('SP','RJ','AP','RR') and datename(month,o.order_delivered_customer_date) is not null



--count of products AND CATEGORY provided by each seller
SELECT distinct s.seller_id,s.seller_state,COUNT( DISTINCT C.customer_unique_id),count(distinct oi.product_id) as product_count,
count( distinct p.product_category_name) as category_count from order_items as oi
inner join sellers as s on s.seller_id=oi.seller_id
inner join customers as c on c.customer_zip_code_prefix=s.seller_zip_code_prefix
inner join products as p on p.product_id=oi.product_id
where c.customer_state in ('SP','RJ','aP','rr')
Group by s.seller_id,s.seller_state 




--Count of ordering from other than their state
select*from(select distinct c.customer_state,c.customer_unique_id,s.seller_state from orders as o
inner join customers as c on c.customer_id=o.customer_id
inner join order_items as oi on oi.order_id=o.order_id
inner join sellers as s on s.seller_id=oi.seller_id
where s.seller_state!=c.customer_state) d 
pivot(count(customer_unique_id) for seller_state in(AC,AM,BA,CE,DF,ES,[GO],MA,MG,MS,MT,PA,PB,PE,[PI],PR,RJ,RN,RO,RS,SC,SE,SP)) as pvt

select  c.customer_state ,count(o.order_id) as Total_order from orders as o 
inner join customers as c on o.customer_id = c.customer_id
group by  c.customer_state

SELECT Z.* ,AA.count_ FROM(select*from(select distinct c.customer_state,c.customer_unique_id,s.seller_state from orders as o
inner join customers as c on c.customer_id=o.customer_id
inner join order_items as oi on oi.order_id=o.order_id
inner join sellers as s on s.seller_id=oi.seller_id
where s.seller_state!=c.customer_state) d 
pivot(count(customer_unique_id) for seller_state 
in(AC,AM,BA,CE,DF,ES,[GO],MA,MG,MS,MT,PA,PB,PE,[PI],PR,RJ,RN,RO,RS,SC,SE,SP)) as pvt) AS Z
INNER JOIN 
(select  c.customer_state ,count(o.order_id) as count_ from orders as o 
inner join customers as c on o.customer_id = c.customer_id
 group by c.customer_state)as AA on AA.customer_state = Z.customer_state



--Category wise sales for states
select c.customer_state,pn.column2,datename(QUARTER,o.order_delivered_customer_date) as month,datename(year,o.order_delivered_customer_date) as year,
COUNT(p.product_id) count_product,sum(oi.price) Total_sum from order_items as oi 
inner join orders as o on o.order_id=oi.order_id
inner join customers as c on c.customer_id=o.customer_id
inner join products as p on p.product_id=oi.product_id
inner join product_category as pn on p.product_category_name=pn.column1
where c.customer_state in ('SP','RJ','AP','RR') and datename(month,o.order_delivered_customer_date) is not null
group by c.customer_state,pn.column2,datename(QUARTER,o.order_delivered_customer_date) ,datename(year,o.order_delivered_customer_date) 
order by c.customer_state

--product wise sales
select c.customer_state,p.product_id,COUNT(p.product_id) count_product,sum(oi.price) as Total_value from order_items as oi 
inner join orders as o on o.order_id=oi.order_id
inner join customers as c on c.customer_id=o.customer_id
inner join products as p on p.product_id=oi.product_id
group by c.customer_state,p.product_id 
order by c.customer_state

--Processing time
select distinct c.customer_state,avg(cast(datediff(day,o.order_purchase_timestamp,o.order_delivered_carrier_date) as float)) as processing_period
,datename(MONTH,o.order_delivered_carrier_date) as month,datename(year,o.order_delivered_carrier_date) as year from orders as o
inner join customers as c on c.customer_id=o.customer_id 
where o.order_delivered_carrier_date is not null and c.customer_state in('SP','RJ','AP','RR') 
group by c.customer_state,datename(MONTH,o.order_delivered_carrier_date),datename(year,o.order_delivered_carrier_date)

--ranking of products
select m.seller_state,m.count_product,m.product_id,x.samestate_count from(select*from(select*,dense_rank()
over(partition by s.seller_state order by count_product desc) AS RANK_ 
from( select s.seller_state,p.product_id,COUNT(p.product_id) count_product from order_items as oi 
inner join sellers as s on s.seller_id=oi.seller_id
inner join products as p on p.product_id=oi.product_id
group by s.seller_state,p.product_id) s) d where RANK_=1) as m
left join 
(select s.seller_state,oi.product_id,count(oi.product_id) as samestate_count  from orders as o
inner join customers as c on o.customer_id=c.customer_id
inner join order_items as oi on oi.order_id=o.order_id
inner join sellers as s on s.seller_id=oi.seller_id
where c.customer_state=s.seller_state group by s.seller_state,oi.product_id) as x on x.product_id=m.product_id and x.seller_state=m.seller_state
order by x.seller_state


--seller category sales
select distinct s.seller_state,datename(QUARTER,o.order_delivered_customer_date) as quarter ,
datename(year,o.order_delivered_customer_date) as year,pn.column2,COUNT(p.product_id) count_product,sum(oi.price) Total_sum 
from order_items as oi 
inner join orders as o on o.order_id=oi.order_id
inner join sellers as s on s.seller_id=oi.seller_id
inner join products as p on p.product_id=oi.product_id
inner join product_category as pn on p.product_category_name=pn.column1
where o.order_delivered_customer_date is not null
group by s.seller_state,s.seller_id,pn.column2,datename(QUARTER,o.order_delivered_customer_date) ,datename(year,o.order_delivered_customer_date)

--product wise sales
select s.seller_state,s.seller_id,pn.column2,COUNT(distinct p.product_id) count_product,sum(oi.price) Total_sum from order_items as oi 
inner join sellers as s on s.seller_id=oi.seller_id
inner join products as p on p.product_id=oi.product_id
inner join product_category as pn on p.product_category_name=pn.column1
group by s.seller_state,s.seller_id,pn.column2


--Count of delivered on time or early 
select distinct c.customer_state,datename(month,o.order_delivered_customer_date) as month,datename(year,o.order_delivered_customer_date) as year,
count(case when o.order_delivered_customer_date<=o.order_estimated_delivery_date then o.order_id end) early_order,
count(case when o.order_delivered_customer_date>o.order_estimated_delivery_date then o.order_id end) as late_orders,
count(o.order_id) as total_orders
from orders as o
inner join customers  as c on c.customer_id=o.customer_id where o.order_status='Delivered' and c.customer_state in ('SP','RJ','AP','RR') 
group by c.customer_state,datename(month,o.order_delivered_customer_date),datename(year,o.order_delivered_customer_date)

--seller order satisfaction on 
select e.seller_state,e.month,e.year,d.Average_time,e.seller_count,e.order_count,d.order_count,d.seller_count 
from(select s.seller_state,datename(month,o.order_delivered_customer_date) as month,datename(year,o.order_delivered_customer_date) as year,
count(oi.order_id) order_count,COUNT(distinct s.seller_id) seller_count from orders as o 
inner join order_items as oi on oi.order_id=o.order_id
inner join sellers as s on s.seller_id=oi.seller_id where o.order_delivered_customer_date is not null 
group by s.seller_state,datename(month,o.order_delivered_customer_date),datename(year,o.order_delivered_customer_date))
as e left join
(select s.seller_state,datename(month,o.order_delivered_customer_date) as month,datename(year,o.order_delivered_customer_date) as year,
AVG(cast(DATEDIFF(DAY,O.order_purchase_timestamp,O.order_delivered_customer_date) as float)) AS Average_time,
count(oi.order_id) order_count,COUNT(distinct s.seller_id) seller_count from orders as o 
inner join order_items as oi on oi.order_id=o.order_id
inner join customers as c on c.customer_id=o.customer_id
inner join sellers as s on s.seller_id=oi.seller_id 
where s.seller_state=c.customer_state and o.order_delivered_customer_date is not null 
group by s.seller_state,datename(month,o.order_delivered_customer_date),datename(year,o.order_delivered_customer_date)) as d 
on d.month=e.month and d.year=e.year and d.seller_state=e.seller_state
where e.seller_state in ('SP','RJ','AP','RR')


--PROCESSING TIME FOR CITY
select distinct c.customer_state,c.customer_city,avg(cast(datediff(day,o.order_purchase_timestamp,o.order_delivered_carrier_date) as float)) as processing_period
,datename(MONTH,o.order_delivered_carrier_date) as month,datename(year,o.order_delivered_carrier_date) as year from orders as o
inner join customers as c on c.customer_id=o.customer_id
where o.order_delivered_carrier_date is not null and c.customer_state in('SP','RJ','AP','RR') 
group by c.customer_state,datename(MONTH,o.order_delivered_carrier_date),datename(year,o.order_delivered_carrier_date),c.customer_city

--Average lead time delivery
select distinct c.customer_state,C.customer_city,datename(month,o.order_delivered_customer_date) as month,
datename(year,o.order_delivered_customer_date) as year,avg(DATEDIFF(DAY,O.order_delivered_customer_date,O.order_estimated_delivery_date))
over(partition by c.customer_state,C.customer_city,datename(month,o.order_delivered_customer_date),
datename(year,o.order_delivered_customer_date)) AS Avg_lead_delivery  from orders as o
inner join customers as c on c.customer_id=o.customer_id
where c.customer_state in('SP','RJ','AP','RR') and datename(month,o.order_delivered_customer_date) is not null


--order early or late
select distinct c.customer_state,c.customer_city,datename(QUARTER,o.order_delivered_customer_date) as month,
datename(year,o.order_delivered_customer_date) as year,
count(case when o.order_delivered_customer_date<=o.order_estimated_delivery_date then o.order_id end) early_order,
count(case when o.order_delivered_customer_date>o.order_estimated_delivery_date then o.order_id end) as late_orders,
count(o.order_id) as total_orders
from orders as o
inner join customers  as c on c.customer_id=o.customer_id
where o.order_status='Delivered' and c.customer_state in ('SP','RJ','AP','RR') 
group by c.customer_state,datename(QUARTER,o.order_delivered_customer_date),datename(year,o.order_delivered_customer_date),c.customer_city

--CATEGORY SALES and city wise
select c.customer_state,pn.column2,c.customer_city,datename(QUARTER,o.order_delivered_customer_date) as month,
datename(year,o.order_delivered_customer_date) as year,COUNT(p.product_id) count_product,sum(oi.price) Total_sum from order_items as oi 
inner join orders as o on o.order_id=oi.order_id
inner join customers as c on c.customer_id=o.customer_id
inner join products as p on p.product_id=oi.product_id
inner join product_category as pn on p.product_category_name=pn.column1
where c.customer_state in ('SP','RJ','AP','RR') and datename(month,o.order_delivered_customer_date) is not null
group by c.customer_state,c.customer_city,pn.column2,datename(QUARTER,o.order_delivered_customer_date) ,
datename(year,o.order_delivered_customer_date) order by c.customer_state

--Review score
select c.customer_state,c.customer_city,datename(MONTH,od.order_delivered_customer_date) as month,
datename(year,od.order_delivered_customer_date) as year,avg(o.review_score) as avg_score,count(o.review_score) as count_reviews 
from order_items as oi
inner join order_reviews as o on o.order_id=oi.order_id
inner join orders as od on od.order_id=oi.order_id
inner join customers as c on c.customer_id=od.customer_id 
where datename(MONTH,od.order_delivered_customer_date) is not null and c.customer_state in ('SP','RJ','AP','RR') 
group by c.customer_state,c.customer_city,datename(MONTH,od.order_delivered_customer_date),datename(year,od.order_delivered_customer_date)