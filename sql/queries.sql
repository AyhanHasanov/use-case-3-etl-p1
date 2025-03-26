DROP DATABASE IF EXISTS ecommerce_db;
CREATE DATABASE IF NOT EXISTS ecommerce_db;
USE ecommerce_db;

CREATE SCHEMA IF NOT EXISTS stage_external;
USE SCHEMA stage_external;

CREATE OR REPLACE STAGE stage_external.ec_stage
URL = 's3://fakecompanydata/'
FILE_FORMAT = (TYPE = CSV);

DROP TABLE IF EXISTS stage_external.raw_orders;
CREATE TEMPORARY TABLE IF NOT EXISTS stage_external.raw_orders(
    order_id VARCHAR(255),
    customer_id VARCHAR(15), 
    customer_name VARCHAR(255), 
    order_date VARCHAR(255), 
    product VARCHAR(255), 
    quantity VARCHAR(255), 
    price VARCHAR(255), 
    discount VARCHAR(255), 
    total_amount VARCHAR(255), 
    payment_method VARCHAR(255),
    shipping_address VARCHAR(255), 
    status VARCHAR(255)
);

COPY INTO stage_external.raw_orders 
FROM @stage_external.ec_stage
FILE_FORMAT = (
    TYPE = CSV,
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    PARSE_HEADER = TRUE
    )
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE';

CREATE SCHEMA IF NOT EXISTS curated;
USE SCHEMA curated;

CREATE TEMPORARY TABLE curated.clean_orders AS
SELECT 
    order_id,
    customer_id,
    customer_name,
    CASE 
        WHEN TRY_CAST(order_date AS DATE) IS NULL THEN '1000-01-01'
        ELSE TRY_CAST(order_date AS DATE)
    END AS order_date,
    product,
    CASE 
        WHEN TRY_CAST(quantity AS NUMBER) IS NULL THEN -1 
        ELSE TRY_CAST(quantity AS NUMBER) 
    END AS quantity,
    CASE 
        WHEN TRY_CAST(price AS DECIMAL(8, 2)) IS NULL THEN -1 
        ELSE TRY_CAST(price AS DECIMAL(8, 2)) 
    END AS price,
    CASE 
        WHEN TRY_CAST(discount AS FLOAT) IS NULL THEN 0 
        ELSE TRY_CAST(discount AS FLOAT) 
    END AS discount,
    CASE 
        WHEN TRY_CAST(total_amount AS DECIMAL(10, 2)) IS NULL THEN price * quantity * (1 - discount)
        ELSE TRY_CAST(total_amount AS DECIMAL(10, 2)) 
    END AS total_amount,
    payment_method,
    shipping_address,
    status
FROM stage_external.raw_orders
WHERE order_id IS NOT NULL;

--
CREATE TEMPORARY TABLE curated.deduplicated_orders AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS row_num
    FROM curated.clean_orders
)
WHERE row_num = 1;

ALTER TABLE curated.deduplicated_orders DROP COLUMN row_num;

--Липсва идентификатор на клиент - фундаментално незавършен запис
CREATE TRANSIENT TABLE curated.td_suspicious_records LIKE curated.clean_orders;

INSERT INTO curated.td_suspicious_records
SELECT *
FROM curated.deduplicated_orders
WHERE customer_id IS NULL;

DELETE FROM curated.deduplicated_orders
WHERE order_id IN (SELECT order_id FROM curated.td_suspicious_records);


--Невалидни дати
CREATE TRANSIENT TABLE curated.td_invalid_date_format LIKE curated.clean_orders;

INSERT INTO curated.td_invalid_date_format
SELECT * FROM curated.deduplicated_orders
WHERE order_date = '1000-01-01';

ALTER TABLE curated.td_invalid_date_format
ADD COLUMN invalid_date VARCHAR(255);

UPDATE curated.td_invalid_date_format
SET invalid_date = stage_external.raw_orders.order_date
FROM stage_external.raw_orders
WHERE curated.td_invalid_date_format.order_id = stage_external.raw_orders.order_id;

--Невалидна цена или количество
CREATE TRANSIENT TABLE IF NOT EXISTS curated.td_invalid_price_or_quantity LIKE curated.clean_orders;

INSERT INTO curated.td_invalid_price_or_quantity
SELECT * FROM curated.deduplicated_orders 
WHERE quantity < 0 OR price < 0;

DELETE FROM curated.deduplicated_orders
WHERE quantity < 0 OR price < 0;

-- Прехвърляне на записите с невалидни статуси и без адреси
CREATE TRANSIENT TABLE curated.td_for_review LIKE curated.clean_orders;

INSERT INTO curated.td_for_review
SELECT *
FROM curated.deduplicated_orders
WHERE shipping_address IS NULL AND status IN ('Delivered', 'Shipped'); 
-- безсмислено е да съществува поръчка, която да е изпратена, без адрес на получаване

DELETE FROM curated.deduplicated_orders
WHERE order_id IN (SELECT order_id FROM curated.td_for_review);

-- Промяна на статуса на поръчките без адрес
ALTER TABLE curated.td_for_review
ADD COLUMN old_status VARCHAR(255);

UPDATE curated.td_for_review
SET old_status = status;

UPDATE curated.td_for_review
SET status = 'Pending';

-- Оправяне на невалидно намаление
UPDATE curated.deduplicated_orders
SET discount = 
    CASE
        WHEN discount < 0 THEN 0
        WHEN discount > 0.5 THEN 0.5
    END
WHERE discount < 0 OR discount > 0.5;   

-- Промяна на метода на плащане там, където то не е зададено.
UPDATE curated.deduplicated_orders 
SET payment_method = 'Unknown'
WHERE payment_method IS NULL;

-- Преизчисляване на крайната цена
UPDATE curated.deduplicated_orders
SET total_amount = ((quantity * price) * (1 - discount));

CREATE SCHEMA IF NOT EXISTS production;
USE SCHEMA production;

CREATE TABLE IF NOT EXISTS orders LIKE curated.clean_orders;
CREATE TRANSIENT TABLE IF NOT EXISTS td_for_review LIKE curated.td_for_review;

-- Наливане на информация в двете таблици, които ще се ползват
--Валидирани
--В тази таблица са всички поръчки
INSERT INTO orders
SELECT * FROM curated.deduplicated_orders;

-- В тази таблица са поръчките, които трябва да бъдат преразгледани и променени
INSERT INTO td_for_review
SELECT * FROM curated.td_for_review;
