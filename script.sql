-- Исходные таблицы

CREATE TABLE raw_data.menu
(
    cafe_name VARCHAR(50),
    menu      jsonb
);
CREATE TABLE raw_data.sales
(
    report_date   date,
    cafe_name     VARCHAR(50),
    type          VARCHAR(50),
    avg_check     NUMERIC(6, 2),
    manager       VARCHAR(50),
    manager_phone VARCHAR(50),
    latitude      double precision,
    longitude     double precision
);
CREATE TABLE cafe.districts
(
    id            serial PRIMARY KEY,
    district_name VARCHAR(255) NOT NULL,
    district_geom GEOMETRY(Geometry, 4326)
);

CREATE EXTENSION PostGIS;


-- Этап 1. Создание дополнительных таблиц


-- Шаг 1. Создание enum типа restaurant_type

CREATE TYPE cafe.restaurant_type AS ENUM ('coffee_shop', 'restaurant', 'bar', 'pizzeria');

-- Шаг 2. Создание таблицы cafe.restaurants с UUID и PostGIS

CREATE TABLE cafe.restaurants
(
    restaurant_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(50) NOT NULL,
    type            cafe.restaurant_type,
    menu            jsonb,
    location        GEOMETRY(Point, 4326)
);

-- Шаг 3. Создание таблицы cafe.managers с UUID

CREATE TABLE cafe.managers
(
    manager_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name         VARCHAR(50) NOT NULL,
    phone        VARCHAR(50)
);

-- Шаг 4. Создание таблицы cafe.restaurant_manager_work_dates

CREATE TABLE cafe.restaurant_manager_work_dates
(
    restaurant_uuid UUID REFERENCES cafe.restaurants (restaurant_uuid),
    manager_uuid    UUID REFERENCES cafe.managers (manager_uuid),
    start_date      DATE NOT NULL,
    end_date        DATE,
    PRIMARY KEY (restaurant_uuid, manager_uuid)
);

-- Шаг 5. Создание таблицы cafe.sales

CREATE TABLE cafe.sales
(
    date            DATE NOT NULL,
    restaurant_uuid UUID REFERENCES cafe.restaurants (restaurant_uuid),
    avg_check       NUMERIC(6, 2),
    PRIMARY KEY (date, restaurant_uuid)
);


-- Этап 2. Создание представлений и написание аналитических запросов


-- Задание 1. Создание представления: топ-3 заведения по среднему чеку в каждом типе

CREATE OR REPLACE VIEW cafe.top_3_restaurants_by_avg_check AS
WITH avg_checks AS (SELECT r.name                                                                 AS restaurant_name,
                           r.type                                                                 AS restaurant_type,
                           ROUND(AVG(s.avg_check), 2)                                             AS avg_check,
                           ROW_NUMBER() OVER (PARTITION BY r.type ORDER BY AVG(s.avg_check) DESC) AS rn
                    FROM cafe.sales s
                             JOIN cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid
                    GROUP BY r.restaurant_uuid, r.name, r.type)
SELECT restaurant_name,
       restaurant_type,
       avg_check
FROM avg_checks
WHERE rn <= 3
ORDER BY restaurant_type, avg_check DESC;


-- Задание 2. Создание материализованного представления: динамика среднего чека по годам

CREATE MATERIALIZED VIEW cafe.restaurant_avg_check_yearly AS
WITH yearly_avg AS (SELECT EXTRACT(YEAR FROM s.date)  AS year,
                           r.restaurant_uuid,
                           r.name                     AS restaurant_name,
                           r.type                     AS restaurant_type,
                           ROUND(AVG(s.avg_check), 2) AS current_avg_check
                    FROM cafe.sales s
                             JOIN cafe.restaurants r ON s.restaurant_uuid = r.restaurant_uuid
                    WHERE EXTRACT(YEAR FROM s.date) != 2023
                      AND s.date < CURRENT_DATE
                    GROUP BY EXTRACT(YEAR FROM s.date), r.restaurant_uuid, r.name, r.type)
SELECT ya.year,
       ya.restaurant_name,
       ya.restaurant_type,
       ya.current_avg_check                                                              AS avg_check_current_year,
       LAG(ya.current_avg_check) OVER (PARTITION BY ya.restaurant_name ORDER BY ya.year) AS avg_check_previous_year,
       ROUND(
               (ya.current_avg_check -
                LAG(ya.current_avg_check) OVER (PARTITION BY ya.restaurant_name ORDER BY ya.year)) * 100.0 /
               NULLIF(LAG(ya.current_avg_check) OVER (PARTITION BY ya.restaurant_name ORDER BY ya.year), 0),
               2
       )                                                                                 AS avg_check_change_percent
FROM yearly_avg ya
ORDER BY ya.restaurant_name, ya.year;


-- Задание 3. Найти топ-3 заведения, где чаще всего менялся менеджер

SELECT r.name                          AS restaurant_name,
       COUNT(DISTINCT rm.manager_uuid) AS manager_change_count
FROM cafe.restaurant_manager_work_dates rm
         JOIN cafe.restaurants r ON rm.restaurant_uuid = r.restaurant_uuid
GROUP BY r.restaurant_uuid, r.name
ORDER BY manager_change_count DESC
LIMIT 3;


-- Задание 4. Найти пиццерию с наибольшим количеством пицц в меню

WITH pizza_entries AS (SELECT r.name              AS restaurant_name,
                              (r.menu -> 'Пицца') AS pizza_menu
                       FROM cafe.restaurants r
                       WHERE r.type = 'pizzeria'
                         AND r.menu ? 'Пицца'),
     pizza_dishes AS (SELECT restaurant_name,
                             dish.key AS dish_name
                      FROM pizza_entries
                               CROSS JOIN jsonb_each_text(pizza_menu) AS dish(key, value)),
     pizza_counts AS (SELECT restaurant_name,
                             COUNT(*) AS pizza_count
                      FROM pizza_dishes
                      GROUP BY restaurant_name),
     ranked_pizzerias AS (SELECT restaurant_name,
                                 pizza_count,
                                 DENSE_RANK() OVER (ORDER BY pizza_count DESC) AS rnk
                          FROM pizza_counts)
SELECT restaurant_name,
       pizza_count
FROM ranked_pizzerias
WHERE rnk = 1
ORDER BY restaurant_name;


-- Задание 5. Найти самую дорогую пиццу для каждой пиццерии

WITH menu_cte AS (SELECT r.name                AS restaurant_name,
                         'Пицца'               AS dish_type,
                         dish.key              AS pizza_name,
                         (dish.value)::NUMERIC AS price
                  FROM cafe.restaurants r
                           CROSS JOIN jsonb_each_text(r.menu -> 'Пицца') AS dish(key, value)
                  WHERE r.type = 'pizzeria'
                    AND r.menu ? 'Пицца'),
     menu_with_rank AS (SELECT restaurant_name,
                               dish_type,
                               pizza_name,
                               price,
                               ROW_NUMBER() OVER (PARTITION BY restaurant_name ORDER BY price DESC) AS rn
                        FROM menu_cte)
SELECT restaurant_name,
       dish_type,
       pizza_name,
       price
FROM menu_with_rank
WHERE rn = 1
ORDER BY restaurant_name;


-- Задание 6. Найти два самых близких друг к другу заведения одного типа

WITH dist AS (SELECT r1.name                                                     AS restaurant_1,
                     r2.name                                                     AS restaurant_2,
                     r1.type                                                     AS restaurant_type,
                     ST_Distance(r1.location::geography, r2.location::geography) AS distance_meters
              FROM cafe.restaurants r1
                       JOIN cafe.restaurants r2
                            ON r1.type = r2.type
                                AND r1.restaurant_uuid != r2.restaurant_uuid
              WHERE r1.location IS NOT NULL
                AND r2.location IS NOT NULL),
     min_distance_cte AS (SELECT restaurant_1,
                                 restaurant_2,
                                 restaurant_type,
                                 MIN(distance_meters) AS min_distance
                          FROM dist
                          GROUP BY restaurant_1, restaurant_2, restaurant_type)
SELECT restaurant_1,
       restaurant_2,
       restaurant_type,
       min_distance
FROM min_distance_cte
ORDER BY min_distance
LIMIT 1;


-- Задание 7. Найти район с самым большим и самым маленьким количеством заведений

WITH district_counts AS (SELECT d.district_name,
                                COUNT(r.restaurant_uuid) AS restaurant_count
                         FROM cafe.districts d
                                  LEFT JOIN cafe.restaurants r
                                            ON ST_Within(r.location, d.district_geom)
                         GROUP BY d.district_name)
SELECT district_name,
       restaurant_count
FROM district_counts
WHERE restaurant_count = (SELECT MAX(restaurant_count) FROM district_counts)

UNION ALL

SELECT district_name,
       restaurant_count
FROM district_counts
WHERE restaurant_count = (SELECT MIN(restaurant_count) FROM district_counts)

ORDER BY restaurant_count DESC;