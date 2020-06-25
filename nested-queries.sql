-- this query serves to return the menu item ID, menu page ID, and dish ID for a particular dish lookup.
select mi.id, mi.menu_page_id, mi.dish_id
from dataset1.Menu_Item as mi 
where mi.id = 
(select d.id from dataset1.Dish d
where d.id = 96)

-- this query returns the rows in Menu_Item with a price higher than the average price of all items.
select *
from dataset1.Menu_Item
where price > 
(select avg(price) from dataset1.Menu_Item)

-- this query returns the avg price of the dish coffee. 
select avg(price) from dataset1.Menu_Item
where dish_id = (select id from dataset1.Dish
where name = 'coffee')

-- this query selects the rows in Dish that exist in Menu_Item
select * from dataset1.Dish d
where exists
(select * from dataset1.Menu_Item mi join dataset1.Menu_Page mp on mi.menu_page_id = mp.id where d.id = mi.dish_id)

-- this query returns the row in menu for the menu with the largest full_height page value.
select * from dataset1.Menu m
where m.id = (select m.id from dataset1.Menu m join dataset1.Menu_Page mp on m.id = mp.menu_id where full_height =
(select max(full_height) from dataset1.Menu_Page))

-- this query returns the row in menu for the menu with the largest full_width page value.
select * from dataset1.Menu m
where m.id = (select m.id from dataset1.Menu m join dataset1.Menu_Page mp on m.id = mp.menu_id where full_width =
(select max(full_width) from dataset1.Menu_Page))

-- this query returns the rows in menu item containing dishes that have a higher than average number of times appeared.
select * from dataset1.Menu_Item mi
where mi.id in (select mi.id from dataset1.Menu_Item mi join dataset1.Dish d on mi.dish_id = d.id where times_appeared >
(select avg(times_appeared) from dataset1.Dish))
 
-- this query returns the id and price of dishes that have appeared on menus more than 1000 times. 
select id, price
from dataset1.Menu_Item
where dish_id in
(select id from dataset1.Dish where menus_appeared > 1000)
