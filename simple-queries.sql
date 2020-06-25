-- returns all items with a price higher than $1000. We were surprised by the amount (1184) of items returned with such a 
-- high price.
select * from dataset1.Menu_Item where price > 1000

-- from the previous query, there was a dish with a price of $3000. we were curious what that dish was, so we constructed 
-- this query to find the name of that dish via its ID.
select * from dataset1.Dish where id = 149674

-- returns the ID and Page Count of Menus with a page count greater than 10, in descending order. The largest menu has 74 pages.
select id, page_count from dataset1.Menu where page_count > 10 order by page_count desc

-- from the previous query, we took the ID of the menu with the largest number of pages. this query returns all information
-- for pages from that menu in ascending order of their height.
select * from dataset1.Menu_Page where menu_id = 35170 order by full_height asc

-- returns all menus with an event listed as 'BANQUET'. 74 menus exist for the occasion of banquets.
select * from dataset1.Menu where event = 'BANQUET'

-- returns the name of a dish and the number of times it has appeared on a menu as long as the dish has appeared more than 
-- 1000 times, in descending order. Unsurprisingly, #1 is Coffee, with 7,744 times appeared.
select name, menus_appeared from dataset1.Dish where menus_appeared > 1000 order by menus_appeared desc
