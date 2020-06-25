-- splitting, joining, and unioning tables by entity type:
-- we found that each of our tables did not violate the design principles we established in class, so we did not
-- need to perform this transformation on our dataset.

-- checking for duplicates: in each table, we checked to see if there were duplicate IDs. We did not find any,
-- so we did not need to perform this transformation on our dataset.
select distinct id
from dataset1.Dish

select distinct id
from dataset1.Menu

select distinct id
from dataset1.Menu_Item

select distinct id
from dataset1.Menu_Page

-- deletions: 
-- delete Menu Items which were associated with dishes that do not exist in the Dish table
delete from dataset1.Menu_Item
where id in (
select mi.id
from dataset1.Menu_Item  mi
Full Outer Join dataset1.Dish d on mi.dish_id = d.id
where d.id is null and mi.dish_id is not null
)

-- delete Menu Pages which were associated with menus that do not exist in the Menu table
delete from `dataset1.Menu_Page` 
where id in (
select mp.id
from dataset1.Menu  m
Full Outer Join dataset1.Menu_Page mp on m.id = mp.menu_id
where mp.menu_id is not null and m.id is null
)

-- delete Menu Items which were associated with pages that do not exist in the Menu_Page table
delete from dataset1.Menu_Item
where id in (
select mi.id
from `dataset1.Menu_Item` mi
Full Outer Join dataset1.Menu_Page mp on mi.menu_page_id = mp.id
where mi.menu_page_id is not null and mp.id is null
)


-- field types: we found that one of our fields (date on Menu) was of type timestamp and needed to be of type date.
-- these transformations are to change that type via casting it to a new table, along with all other information.
CREATE TABLE dataset1.Menu1 AS 
SELECT *, EXTRACT(DATE FROM date) AS date1 FROM `dataset1.Menu` 

CREATE TABLE dataset1.Menu2 AS
SELECT
     * EXCEPT(date)
   FROM
     dataset1.Menu1
     
     
-- new menu table
CREATE TABLE dataset1.Menu AS 
SELECT id, name, venue, place, event, physical_description, occasion, call_number, keywords, language, notes, sponsor, status, page_count, dish_count, date1 as date, dishes_per_page FROM `dataset1.Menu_1` 

-- new location table 
CREATE TABLE dataset1.Location AS 
SELECT id as menu_id, location, location_type FROM `dataset1.Menu_1` 

-- new currency table
CREATE TABLE dataset1.Currency AS 
SELECT id as menu_id, currency, currency_symbol FROM `dataset1.Menu_1` 

-- new dish table to keep naming conventions
CREATE TABLE dataset1.Dish AS 
SELECT * FROM `dataset1.Dish1` 

-- new menu_page table to keep naming conventions
CREATE TABLE dataset1.Menu_Page AS 
SELECT * FROM `dataset1.Menu_Page1` 
