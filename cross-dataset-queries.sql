-- We will use dataset2.Review to average the ratings for a particular establishment. Then, we will join to 
-- dataset2.Business to see if the average rating listed is accurate for a business.
Select avg(r.stars) as average_rating, avg(b.stars) as reported_rating
from `dogwood-theorem-230119.dataset2.Review` as r
join `dogwood-theorem-230119.dataset2.Business_1` as b on r.business_id = b.business_id
where r.business_id = "ArQs--vHJtN7_m6hGH9EmQ"
group by r.business_id

-- The query will join name on datset2.Business to place in dataset1.Menu, then dataset1.Menu to dataset1.Menu_Page on 
-- menu_id to determine if the ratings on a restaurant have any correlation to the page orientation of their menu. 
SELECT b.newname, b.stars, mp.orientation
FROM `dogwood-theorem-230119.dataset1.Menu` m
JOIN `dogwood-theorem-230119.dataset2.Business_1` b on UPPER(m.place) = UPPER(b.newname)
JOIN `dogwood-theorem-230119.dataset1.Menu_Page` mp on mp.menu_id = m.id
ORDER BY b.stars DESC

-- The query will join name in dataset2.Business to place in dataset1.Menu and it will print out dish_count and stars
-- for each business. We will see if having fewer dishes on the menu correlates with having a higher rating. 
SELECT b.newname, m.dish_count, b.stars
FROM `dogwood-theorem-230119.dataset1.Menu` m
JOIN `dogwood-theorem-230119.dataset2.Business_1` b on UPPER(m.place) = UPPER(b.newname)
WHERE dish_count is not null 

-- The query will join dataset2.Friend to dataset2.User to find out if the count of a users friends correlates with 
-- their total number of reviews given.
select count(f.user_id) as UserCount, avg(u.review_count) as ReviewCount
from `dogwood-theorem-230119.dataset2.Friend` as f
join `dogwood-theorem-230119.dataset2.User` as u on u.user_id = f.user_id
where f.user_id = "Tzdu8NMT7Sv87HLkpHl1LA"

-- The query will join name on dataset2.Business to place in dataset1.Menu to print the dishes_per_page and ratings for 
-- each business. This will see if a more cluttered menu correlates with having a lower rating.
SELECT b.newname, m.dishes_per_page, b.stars
FROM `dogwood-theorem-230119.dataset1.Menu` m
JOIN `dogwood-theorem-230119.dataset2.Business_1` b on UPPER(m.place) = UPPER(b.newname)
WHERE dishes_per_page is not null 
ORDER BY stars DESC

-- The query will join dataset2.Friend to dataset2.Review to find reviews made by both a user and a friend on the same date 
-- to see if the ratings were the same.
SELECT f.user_id, f.friend_id, r.stars as user_rating, fr.stars as friend_rating
FROM `dogwood-theorem-230119.dataset2.Friend` f
JOIN `dogwood-theorem-230119.dataset2.Review` r on f.user_id = r.user_id
JOIN `dogwood-theorem-230119.dataset2.Review` fr on f.friend_id=fr.user_id and fr.stars=r.stars and r.date=fr.date and r.business_id=fr.business_id
