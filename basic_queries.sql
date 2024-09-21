
-- 1. List all unique cities where customers are located. 

select distinct(customer_city) from customers;
-- -----------------------------------------------
-- 2. Count the number of orders placed in 2017.
select count(order_id) as order_count_2017 from orders 
where year(order_approved_at) = 2017;
-- ---------------------------------------------
-- 3. Find the total sales per category.
select products.product_category category , round(sum(payments.payment_value),2) sales
from products 
join order_items 
on products.product_id = order_items.product_id
join payments 
on order_items.order_id = payments.order_id
group by category;
-- -------------------------------------------------------
-- 4. Calculate the percentage of orders that were paid in installments.

select (sum(case when payment_installments >= 1 then 1 else 0 end)/count(*))* 100 as percentage_orders_installments 
from payments;
-- --------------------------------------------------------------------
-- 5. Count the number of customers from each state. 

select customer_state , count(customer_id) as number_customers
from customers 
group by customer_state;


