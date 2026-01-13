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
)

