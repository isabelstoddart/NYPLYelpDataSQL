-- Find all menu item ID and page ID information related to a specific dish (Dish ID 96), the dish's name, and the number
-- of times it appeared on menu pages (to verify that the number of rows printed out is correct).
-- this helps put all the information together for the dish ID 96, which is the dish coffee
select mi.id, mi.menu_page_id, mi.dish_id, d.id, d.name, d.times_appeared
from dataset1.Menu_Item as mi
join dataset1.Dish as d on mi.dish_id = d.id
where dish_id = 96

-- find all information for menu items and their respective dishes where the number of times a dish appears on a menu is
-- greater than 2,500.
-- this helps put all the information together for these dishes that appear more than 2500 times. 
select *
from dataset1.Dish as d
join dataset1.Menu_Item as mi on d.id = mi.dish_id
where menus_appeared > 2500

-- Find height and width for all pages from a particular menu (ID 26453), the location that the menu is for, the IDs, and 
-- the page count for the menu (to make sure the number of rows printed out from Menu_Page is correct).
select m.id, location, page_count, pg.id, full_height, full_width
from dataset1.Menu as m
join dataset1.Menu_Page as pg on m.id = pg.menu_id
where m.id = 26453

-- Find the name of the dish, the id of the dish, and the id of the menu item for the dishes in menu item that don't have an id in dish
-- this helps find the dishes included in a menu item that don't show up in the table of dishes
select d.name, mi.dish_id, d.id
from `dataset1.Menu_Item`  mi
Full Outer Join dataset1.Dish d on mi.dish_id = d.id
where d.id is null and mi.dish_id is not null

-- Find the name of the menu, the menu id, and the menu id of the menu page for the menu pages that don't correlate to a menu in the menu 
-- table.
-- This helps find the menu pages that don't have a respective menu in the menu table
select m.name, mp.menu_id, m.id
from `dataset1.Menu`  m
Full Outer Join dataset1.Menu_Page mp on m.id = mp.menu_id
where mp.menu_id is not null and m.id is null

-- Find the menu page id and the menu page id in the menu item table for the menu items on menu pages that aren't in the menu page table
select mi.menu_page_id, mp.id
from `dataset1.Menu_Item` mi
Full Outer Join dataset1.Menu_Page mp on mi.menu_page_id = mp.id
where mi.menu_page_id is null and mp.id is not null
