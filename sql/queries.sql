DROP DATABASE IF EXISTS ecomerse_db_etl;
CREATE DATABASE IF NOT EXISTS ecomerse_db_etl;
USE ecomerse_db_etl;

CREATE SCHEMA IF NOT EXISTS stage_external;
USE SCHEMA stage_external;

CREATE OR REPLACE STAGE stage_external.ec_stage
URL = 's3://fakecompanydata/'
FILE_FORMAT = (TYPE = CSV);

DROP TABLE IF EXISTS stage_external.temp_orders;
CREATE TEMPORARY TABLE IF NOT EXISTS stage_external.temp_orders(
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

COPY INTO stage_external.temp_orders 
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

CREATE TRANSIENT TABLE curated.clean_orders AS
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
FROM stage_external.temp_orders
WHERE order_id IS NOT NULL;

CREATE TEMPORARY TABLE curated.deduplicated_orders AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS row_num
    FROM curated.clean_orders
)
WHERE row_num = 1;

ALTER TABLE curated.deduplicated_orders DROP COLUMN row_num;

--Ако Адреса за доставка липсва, но статуса е Delivered - прехвърлете записа към отделна таблица, която да съдържа само и единствено такива доставки, гонови за ревю, td_for_review
CREATE TEMPORARY TABLE curated.td_for_review LIKE curated.deduplicated_orders;

INSERT INTO curated.td_for_review
SELECT *
FROM curated.deduplicated_orders
WHERE shipping_address IS NULL AND status = 'Delivered';

DELETE FROM curated.deduplicated_orders
WHERE order_id IN (SELECT order_id FROM curated.td_for_review);

--Ако в записа липсва данни за клиента Customer_id , то тогава този запис трябва да бъде прехвърлен към таблица td_suspisios_records
CREATE TEMPORARY TABLE curated.td_suspicious_records LIKE curated.deduplicated_orders;

INSERT INTO curated.td_suspicious_records
SELECT *
FROM curated.deduplicated_orders
WHERE customer_id IS NULL;

DELETE FROM curated.deduplicated_orders
WHERE order_id IN (SELECT order_id FROM curated.td_suspicious_records);

--Ако липсва информация за платежния метод, коригирайте със стойност по подразбиране Unknown
UPDATE curated.deduplicated_orders 
SET payment_method = 'Unknown'
WHERE payment_method IS NULL;

--Някой от записите имат грешен формат, спрямо другите данни - ще откриете сами за какво говорим. Всеки запис с невалиден формат трябва да се прехвърли към таблица td_invalid_date_format , а финалните записи да се коригират, така че да бъдат в правилния формат.
CREATE TEMPORARY TABLE curated.td_invalid_date_format LIKE curated.deduplicated_orders;

ALTER TABLE curated.td_invalid_date_format
ADD COLUMN invalid_date VARCHAR(255);

INSERT INTO curated.td_invalid_date_format
SELECT
    dor.order_id, 
    dor.customer_id, 
    dor.customer_name, 
    dor.order_date, 
    dor.product, 
    dor.quantity, 
    dor.price, 
    dor.discount, 
    dor.total_amount, 
    dor.payment_method, 
    dor.shipping_address, 
    dor.status,
    tor.order_date AS "invalid_date"
FROM curated.deduplicated_orders dor
JOIN stage_external.temp_orders tor
ON dor.order_id = tor.order_id
WHERE dor.order_date = '1000-01-01'
UNION
SELECT 
    dor.order_id, 
    dor.customer_id, 
    dor.customer_name, 
    dor.order_date, 
    dor.product, 
    dor.quantity, 
    dor.price, 
    dor.discount, 
    dor.total_amount, 
    dor.payment_method, 
    dor.shipping_address, 
    dor.status,
    tor.order_date AS "invalid_date"
FROM curated.td_for_review dor
JOIN stage_external.temp_orders tor
ON dor.order_id = tor.order_id
WHERE dor.order_date = '1000-01-01'
UNION 
SELECT 
    dor.order_id, 
    dor.customer_id, 
    dor.customer_name, 
    dor.order_date, 
    dor.product, 
    dor.quantity, 
    dor.price, 
    dor.discount, 
    dor.total_amount, 
    dor.payment_method, 
    dor.shipping_address, 
    dor.status,
    tor.order_date AS "invalid_date"
FROM curated.td_suspicious_records dor
JOIN stage_external.temp_orders tor
ON dor.order_id = tor.order_id
WHERE dor.order_date = '1000-01-01';

DELETE FROM curated.deduplicated_orders
WHERE order_id IN (SELECT order_id FROM curated.td_invalid_date_format);

--Изтриите всички редове, които съдържат невалидни стойности за цена и количество поръчана стока. Тези стойности винаги трябва да бъдат положителни. Прехвърлете заподозрените записи в специфична таблица, с име по ваш избор.
SELECT * FROM curated.deduplicated_orders 
WHERE quantity < 0 OR price < 0;



