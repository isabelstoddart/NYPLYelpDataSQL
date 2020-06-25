-- gets the total amount of times a dish appeared on a menu grouped by dish name
select count(times_appeared) as times_appeared, name
from dataset1.Dish
group by name

-- gets the dish name and the average price of menu items containing that dish grouping by dish. Only prints out prices that are not null
-- and that are greater than 0 
select d.name, avg(mi.price) as avg_price
from dataset1.Menu_Item as mi
join dataset1.Dish as d on mi.dish_id = d.id
group by d.name 
having avg(mi.price) is not Null and avg(mi.price) > 0.0

-- gets the number of menus in each certian location, prints out location and number of menus grouping by location
select location, count(menu_id) as menus_from_location
from dataset1.Location
group by location

-- gets the number of menus from each venue, omits menus that don't have a venue (that are null). Prints out venue and number of menus 
-- grouping by venue
select venue, count(id) as menus_from_venue
from dataset1.Menu
group by venue
having venue is not NULL

-- gets the times a currency appears in the menu database
select c.currency, count(currency_symbol) as times_appeared
from `dataset1.Currency` as c
group by c.currency

-- selects the highest dollar amount in the high_price column on menu_item
select max(high_price) as highest_price
from `dataset1.Menu_Item` 

-- returns the number of distinct locations
select count(DISTINCT location) as locations
from `dataset1.Location`
having locations is not NULL

-- returns the average height and average width of all menu pages
select avg(full_height) as avg_height, avg(full_width) as avg_width
from `dataset1.Menu_Page`
