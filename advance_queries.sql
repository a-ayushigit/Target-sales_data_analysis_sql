--  1. Calculate the moving average of order values for each customer over their order history.
select customer_id , order_purchase_timestamp , payment , 
avg(payment) over(partition by customer_id order by order_purchase_timestamp
rows between 2 preceding and current row ) as mov_avg
from
(select orders.customer_id , orders.order_purchase_timestamp , payments.payment_value as payment
from orders join payments
on orders.order_id = payments.order_id ) as a;
-- 2. Calculate the cumulative sales per month for each year.
select years , months , sum(payment)
over(order by years , months) as cumulative_sales from
(select year(orders.order_purchase_timestamp) as years, month( orders.order_purchase_timestamp) as months , 
round(sum(payments.payment_value),2) as payment 
from orders join payments 
on payments.order_id = orders.order_id
group by years , months 
order by years , months ) as a;
-- 3. Calculate the year-over-year growth rate of total sales.


with a as (select year(orders.order_purchase_timestamp) as years, 
round(sum(payments.payment_value),2) as payment 
from orders join payments 
on payments.order_id = orders.order_id
group by years  
order by years)  
select years , ((payment - lag(payment , 1) over (order by years))*100/ lag(payment , 1) over (order by years) ) as yoy_growth_percent from a ;

-- 4. Identify the top 3 customers who spent the most money in each year.
select years , customer_id , round(payment,2) as payment , d_rank from (select year(orders.order_purchase_timestamp) as years , 
orders.customer_id,
sum(payments.payment_value) payment , 
dense_rank() over(partition by year(orders.order_purchase_timestamp) order by sum(payments.payment_value) desc ) d_rank
from orders join payments 
on payments.order_id = orders.order_id
group by years , orders.customer_id ) as a
where d_rank <= 3; 

