/*
Upstart13 Case study

Purpose
------------------------------------------
I transform the raw CSV ingestion layer (raw.*) into typed tables (store.*) and then created curated tables to answer the questions (publish.*).

Notes
------------------------------------------
I kept the data exploration and validation queries in the script on purpose. In a real project I would not put this amount of comments on a .sql file.

Developing
------------------------------------------
row count check matching csvs

SELECT 'raw_products' AS tabela, COUNT(*) AS linhas FROM raw.raw_products -- 303
UNION ALL
SELECT 'raw_sales_order_detail', COUNT(*) FROM raw.raw_sales_order_detail -- 121,317
UNION ALL
SELECT 'raw_sales_order_header', COUNT(*) FROM raw.raw_sales_order_header; -- 31465
*/

/*
products check

select * from raw.raw_products limit 10;
select count(*) as count from raw.raw_products

select count(distinct CASE WHEN "ProductID" <> '' then "ProductID" END) AS pkcheck from raw.raw_products -- diverged from total count

IDs 713, 714, 715, 716, 881, 882, 883, 884 are duplicated. In this case we have 3 options: deduplicate, pivot the
productsubcategoryname from the duplicates into a third column in silver when product id, product desc and product number are all equal. Or,
as a last option, we could create a new dimension for the product subcategory subdivisions to join on, a sort of bridge.
Since this productid column will be used later for joins, I'll go with deduplication keeping the most complete row.

update on dedup: after looking at the ProductDesc for all 8 duplicates, they all say "Jersey" in the name ("Long-Sleeve Logo Jersey, S", "Short-Sleeve Classic Jersey, XL").
the row with SubCategory='Shirt' and Category='Clothing' has the wrong subcategory, they're jerseys not shirts.
the row with SubCategory='Jerseys' has the right subcategory but no category.
so: keep the row with category filled (Clothing is correct), then UPDATE the specific IDs from Shirt to Jerseys after.

select "ProductID", count(*) AS qt from raw.raw_products where "ProductID" <> '' group by "ProductID" having count(*) > 1 order by qt DESC;
select * from raw.raw_products where "ProductID" in ('881','884','713','714','882','716','883','715') ORDER BY "ProductID" desc;
select distinct "ProductCategoryName" from raw.raw_products
select distinct "MakeFlag" from raw.raw_products

select max(length("ProductDesc")) from raw.raw_products
select max(length("ProductSubCategoryName")) from raw.raw_products
select max(length("ProductNumber")) from raw.raw_products
*/
DO $$
DECLARE v_rows bigint;
BEGIN

RAISE NOTICE '---TL START---';


-- ================================
-- SILVER - store_products
-- ================================

CREATE TABLE IF NOT EXISTS store.store_products (
product_id                  INTEGER         NOT NULL,
product_desc                VARCHAR(50)     NOT NULL, --max length was 32
product_number              VARCHAR(15)     NOT NULL,
make_flag                   BOOLEAN         NOT NULL,
color                       VARCHAR(50),
safety_stock_level          INTEGER        NOT NULL,
reorder_point               INTEGER        NOT NULL,
standard_cost               NUMERIC(10,4)   NOT NULL,
list_price                  NUMERIC(10,4)   NOT NULL,
size                        VARCHAR(5),
size_unit_measure_code      VARCHAR(5),
weight                      NUMERIC(8,2),
weight_unit_measure_code    VARCHAR(5),
product_category_name       VARCHAR(50),
product_sub_category_name   VARCHAR(50)     NOT NULL,
CONSTRAINT pk_store_products PRIMARY KEY (product_id)
);

RAISE NOTICE '[1/5] store.store_products: Table created.';

TRUNCATE TABLE store.store_products CASCADE;

RAISE NOTICE '[1/5] store.store_products: Table truncated.';

/* :: casts to avoid implicit conversion and dedup rosw with ProductCategoryName filled (most complete in my opinion) */
INSERT INTO
  store.store_products
SELECT
  "ProductID":: INTEGER AS product_id,
  "ProductDesc" AS product_desc,
  "ProductNumber" AS product_number,
  "MakeFlag":: BOOLEAN AS make_flag,
  NULLIF(TRIM("Color"), '') AS color,
  "SafetyStockLevel":: INTEGER AS safety_stock_level,
  "ReorderPoint":: INTEGER AS reorder_point,
  "StandardCost":: NUMERIC(10, 4) AS standard_cost,
  "ListPrice":: NUMERIC(10, 4) AS list_price,
  NULLIF(TRIM("Size"), '') AS size,
  NULLIF(TRIM("SizeUnitMeasureCode"), '') AS size_unit_measure_code,
  NULLIF(TRIM("Weight"), ''):: NUMERIC(8, 2) AS weight,
  NULLIF(TRIM("WeightUnitMeasureCode"), '') AS weight_unit_measure_code,
  NULLIF(TRIM("ProductCategoryName"), '') AS product_category_name,
  "ProductSubCategoryName" AS product_sub_category_name
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY "ProductID"
        ORDER BY
          CASE
            WHEN NULLIF(TRIM("ProductCategoryName"), '') IS NOT NULL THEN 0
            ELSE 1
          END,
          ctid
      ) AS rn
    FROM
      raw.raw_products
  ) deduped
WHERE
  rn = 1;

GET DIAGNOSTICS v_rows = ROW_COUNT;
RAISE NOTICE '[1/5] store.store_products: loaded with % rows', v_rows;

/* deduped rows kept SubCategory='Shirt' because that row had the category filled, but ProductDesc says "Jersey" for all of them, so the correct subcategory in my opinion is 'Jerseys'.
   I used the specific IDS instead of just where sub_category = 'Shirt' on purpose, if there were other legitimate Shirt products in the table I wouldn't want to touch those. */
UPDATE store.store_products
SET product_sub_category_name = 'Jerseys'
WHERE product_id IN (713, 714, 715, 716, 881, 882, 883, 884)
  AND product_sub_category_name = 'Shirt';

RAISE NOTICE '[1/5] store.store_products: corrected Shirt to Jerseys (8 rows).';

--select * from store.store_products


/*
raw_sales_order_header checks

SELECT
count(*) AS count,
sum(case WHEN "SalesOrderID" = '' OR "SalesOrderID" IS NULL THEN 1 ELSE 0 END) as empty,
count(distinct CASE WHEN "SalesOrderID" <> '' THEN "SalesOrderID" END) as distinct_count,
max(length("SalesOrderID")) filter (WHERE "SalesOrderID" <> '') as maxLen
FROM raw.raw_sales_order_header

select * from raw.raw_sales_order_header
select cast("OrderDate" as DATE) as data from raw.raw_sales_order_header
select distinct("OrderDate") as data from raw.raw_sales_order_header order by data asc;
note: OrderDate is broken. if I try to convert to DATE in silver it'll error out, I'll assume it refers to the first day of the month to not break the majority pattern.
select max(length("AccountNumber")) from raw.raw_sales_order_header
Select distinct("OnlineOrderFlag") from raw.raw_sales_order_header
note 2: many null and duplicated salespersonIDs, but the behavior seems normal for a sales person ID since this table is basically an order/invoice header. what can't repeat is the order number itself.
select "SalesPersonID", count(*) AS qt from raw.raw_sales_order_header where "SalesPersonID" <> '' group by "SalesPersonID" having count(*) > 1 order by qt DESC;
*/

-- ================================
-- SILVER - store_sales_order_header
-- ================================

CREATE TABLE IF NOT EXISTS store.store_sales_order_header (
sales_order_id     INTEGER         NOT NULL,
order_date         DATE            NOT NULL,
ship_date          DATE            NOT NULL,
online_order_flag  BOOLEAN         NOT NULL,
account_number     VARCHAR(20)     NOT NULL,
customer_id        INTEGER         NOT NULL,
sales_person_id     INTEGER,
freight            NUMERIC(10,4)   NOT NULL,

    CONSTRAINT pk_store_sales_order_header PRIMARY KEY (sales_order_id)
);

RAISE NOTICE '[2/5] store.store_sales_order_header: Table created.';

TRUNCATE TABLE store.store_sales_order_header CASCADE;

RAISE NOTICE '[2/5] store.store_sales_order_header: Table truncated.';

INSERT INTO store.store_sales_order_header
SELECT
    "SalesOrderID"::INTEGER                                     AS sales_order_id,
    CASE
        WHEN LENGTH(TRIM("OrderDate")) = 7
        THEN (TRIM("OrderDate") || '-01')::DATE                 --converts to day 01 to not break the majority pattern
        ELSE TRIM("OrderDate")::DATE
    END                                                       AS order_date,
    "ShipDate"::DATE                                          AS ship_date,
    "OnlineOrderFlag"::BOOLEAN                                 AS online_order_flag,
    "AccountNumber"                                           AS account_number,
    "CustomerID"::INTEGER                                       AS customer_id,
    NULLIF(TRIM("SalesPersonID"), '')::INTEGER                 AS sales_person_id,
    "Freight"::NUMERIC(10,4)                                    AS freight
FROM raw.raw_sales_order_header;

GET DIAGNOSTICS v_rows = ROW_COUNT;
RAISE NOTICE '[2/5] store.store_sales_order_header: loaded with % rows', v_rows;


--SELECT * FROM store.store_sales_order_header


/*
raw_sales_order_detail checks

select * from raw.raw_sales_order_detail
select max(length("UnitPriceDiscount")) from raw.raw_sales_order_detail --5

checks SalesOrderID field with prep query to find key:
SELECT
count(*) AS count,
sum(case WHEN "SalesOrderID" = '' OR "SalesOrderID" IS NULL THEN 1 ELSE 0 END) as empty,
count(distinct CASE WHEN "SalesOrderID" <> '' THEN "SalesOrderID" END) as distinct_count,
max(length("SalesOrderID")) filter (WHERE "SalesOrderID" <> '') as maxLen
FROM raw.raw_sales_order_detail

checks SalesOrderDetailID field with prep query to find key:
SELECT
count(*) AS count,
sum(case WHEN "SalesOrderDetailID" = '' OR "SalesOrderDetailID" IS NULL THEN 1 ELSE 0 END) as empty,
count(distinct CASE WHEN "SalesOrderDetailID" <> '' THEN "SalesOrderDetailID" END) as distinct_count,
max(length("SalesOrderDetailID")) filter (WHERE "SalesOrderDetailID" <> '') as maxLen
FROM raw.raw_sales_order_detail

checks ProductID field with prep query to find key:
SELECT
count(*) AS count,
sum(case WHEN "ProductID" = '' OR "ProductID" IS NULL THEN 1 ELSE 0 END) as empty,
count(distinct CASE WHEN "ProductID" <> '' THEN "ProductID" END) as distinct_count,
max(length("ProductID")) filter (WHERE "ProductID" <> '') as maxLen
FROM raw.raw_sales_order_detail

note: PK for this table is SalesOrderDetailID and SalesOrderID will be the FK to join with header and ProductID will be the FK to the product dimension

join tests:
SELECT * FROM raw.raw_sales_order_detail AS A
LEFT JOIN raw.raw_sales_order_header AS B
ON A."SalesOrderID" = B."SalesOrderID" --121317

SELECT * FROM raw.raw_sales_order_detail AS A
LEFT JOIN store.store_products AS B
ON A."ProductID"::integer = B."product_id"; --121317

SELECT * FROM store.store_products

select distinct("UnitPriceDiscount") from raw.raw_sales_order_detail

sanity check on unit_price and unit_price_discount before the gold calc:
select count(*) from raw.raw_sales_order_detail where "UnitPrice"::numeric < 0;             -- 0
select count(*) from raw.raw_sales_order_detail where "UnitPriceDiscount"::numeric < 0;     -- 0
select count(*) from raw.raw_sales_order_detail where "UnitPriceDiscount"::numeric > "UnitPrice"::numeric; -- 0
the only negative total_line_extended_price values come from order_qty = -1, makes sense for me because it can be a returns
*/

-- ================================
-- SILVER - store_sales_order_detail
-- ================================

CREATE TABLE IF NOT EXISTS store.store_sales_order_detail (
sales_order_id          INTEGER         NOT NULL,
sales_order_detail_id   INTEGER         NOT NULL,
order_qty               INTEGER        NOT NULL,
product_id              INTEGER        NOT NULL,
unit_price              NUMERIC(10,4)   NOT NULL,
unit_price_discount     NUMERIC(5,4)    NOT NULL,

CONSTRAINT pk_store_sales_order_detail PRIMARY KEY (sales_order_detail_id),
CONSTRAINT fk_detail_to_header FOREIGN KEY (sales_order_id)
	REFERENCES store.store_sales_order_header (sales_order_id),
CONSTRAINT fk_detail_to_product FOREIGN KEY (product_id)
	REFERENCES store.store_products (product_id)
);

RAISE NOTICE '[3/5] store.store_sales_order_detail: Table created.';

TRUNCATE TABLE store.store_sales_order_detail CASCADE;

RAISE NOTICE '[3/5] store.store_sales_order_detail: Table truncated.';

INSERT INTO store.store_sales_order_detail
SELECT
    "SalesOrderID"::INTEGER                                     AS sales_order_id,
    "SalesOrderDetailID"::INTEGER                               AS sales_order_detail_id,
    "OrderQty"::INTEGER                                        AS order_qty,
    "ProductID"::INTEGER                                        AS product_id,
    "UnitPrice"::NUMERIC(10,4)                                  AS unit_price,
    "UnitPriceDiscount"::NUMERIC(5,4)                           AS unit_price_discount
FROM raw.raw_sales_order_detail;

GET DIAGNOSTICS v_rows = ROW_COUNT;
RAISE NOTICE '[3/5] store.store_sales_order_detail: loaded with % rows', v_rows;


------------------------#PUBLISH DIMENSION - publish_product#--------------------------------

CREATE TABLE IF NOT EXISTS publish.publish_product (
product_id                INTEGER        NOT NULL,
product_desc              VARCHAR(50)    NOT NULL,
product_number            VARCHAR(15)    NOT NULL,
make_flag                 BOOLEAN        NOT NULL,
color                     VARCHAR(50)    NOT NULL, -- was nullable in silver, now not null
safety_stock_level        INTEGER       NOT NULL,
reorder_point             INTEGER       NOT NULL,
standard_cost             NUMERIC(10,4)  NOT NULL,
list_price                NUMERIC(10,4)  NOT NULL,
size                      VARCHAR(5),
size_unit_measure_code    VARCHAR(5),
weight                    NUMERIC(8,2),
weight_unit_measure_code  VARCHAR(5),
product_category_name     VARCHAR(50),
product_sub_category_name VARCHAR(50)    NOT NULL,
CONSTRAINT pk_publish_product PRIMARY KEY (product_id)
);

RAISE NOTICE '[4/5] publish.publish_product: Table created.';

TRUNCATE TABLE publish.publish_product CASCADE;

RAISE NOTICE '[4/5] publish.publish_product: Table truncated.';

INSERT INTO publish.publish_product
SELECT
  product_id, 
  product_desc,
  product_number,
  make_flag,
  COALESCE(color, 'N/A') AS color, --color set to N/A when null
  safety_stock_level,
  reorder_point,
  standard_cost,
  list_price,
  size,
  size_unit_measure_code,
  weight,
  weight_unit_measure_code,
  CASE
    WHEN product_category_name IS NOT NULL THEN product_category_name
    WHEN product_sub_category_name IN ('Gloves', 'Shorts', 'Socks', 'Tights', 'Vests') THEN 'Clothing'
    WHEN product_sub_category_name IN ('Locks','Lights','Headsets','Helmets','Pedals','Pumps') THEN 'Accessories'
    WHEN product_sub_category_name LIKE '%Frames%' OR product_sub_category_name IN ('Wheels', 'Saddles') THEN 'Components'
    ELSE 'Others' -- some products dont match any rule above,things like caps,bib-shorts,forks,bike racks,etc
  END AS product_category_name,
  product_sub_category_name
FROM
  store.store_products;

GET DIAGNOSTICS v_rows = ROW_COUNT;
RAISE NOTICE '[4/5] publish.publish_product: loaded with % rows', v_rows;

 --SELECT COUNT(*) FROM   store.store_products;


------------------------#PUBLISH FACT - publish_orders#--------------------------------

CREATE TABLE IF NOT EXISTS publish.publish_orders (
    --SalesOrderDetail fields below
    sales_order_detail_id           INTEGER NOT NULL,
    sales_order_id                  INTEGER NOT NULL,
    order_qty                       INTEGER NOT NULL,
    product_id                      INTEGER NOT NULL,
    unit_price                      NUMERIC(10, 4) NOT NULL,
    unit_price_discount             NUMERIC(5, 4) NOT NULL,
    --SalesOrderHeader fields below (EXCEPT SalesOrderId)
    order_date                      DATE NOT NULL,
    ship_date                       DATE NOT NULL,
    online_order_flag               BOOLEAN NOT NULL,
    account_number                  VARCHAR(20) NOT NULL,
    customer_id                     INTEGER NOT NULL,
    sales_person_id                 INTEGER,
    total_order_freight             NUMERIC(10, 4) NOT NULL, -- renamed from freight
    --calculated fields below
    lead_time_in_business_days      INTEGER NOT NULL,
    total_line_extended_price       NUMERIC(12, 4) NOT NULL,
    CONSTRAINT pk_publish_orders PRIMARY KEY (sales_order_detail_id),
    CONSTRAINT fk_orders_to_product FOREIGN KEY (product_id) REFERENCES publish.publish_product (product_id)
);

RAISE NOTICE '[5/5] publish.publish_orders: Table created.';

TRUNCATE TABLE publish.publish_orders CASCADE;

RAISE NOTICE '[5/5] publish.publish_orders: Table truncated.';


--SELECT generate_series(date '2026-02-01', date '2026-02-05', interval '1 day') AS day;

INSERT INTO
    publish.publish_orders
SELECT
    --Detail
    d.sales_order_detail_id,
    d.sales_order_id,
    d.order_qty,
    d.product_id,
    d.unit_price,
    d.unit_price_discount,
    --Header (except sales_order_id)
    h.order_date,
    h.ship_date,
    h.online_order_flag,
    h.account_number,
    h.customer_id,
    h.sales_person_id,
    h.freight AS total_order_freight,
    /*
    had a question here about whether to include or exclude the order_date and ship_date endpoints when counting business days.
    used generate_series to build the calendar between the two dates and count only weekdays.
    generate_series lists every day in the range, EXTRACT(DOW) gets the day of the week (0=sunday, 6=saturday)
    and NOT IN (0,6) removes the weekends.

    ended up including both the order day and the ship day because ship_date is the day the order actually left.
    if I used order_date + 1 or ship_date - 1, it would change which endpoint gets excluded and that
    changes the result when order_date falls on a weekend, like a saturday.

    thought about it like real life: if I buy something on saturday, saturday and sunday don't count as business days. if the lead time is 3 business days, I'd expect delivery on wednesday, not tuesday.
    that's how I decided to include both endpoints, also because the rule literally says "LeadTimeInBusinessDays as the difference between OrderDate and ShipDate". in SQL, BETWEEN includes both ends.
    */
    (
    SELECT COUNT(*)
    FROM generate_series(
        h.order_date,                     -- starts
        h.ship_date,                      -- ends
        INTERVAL '1 day'                  -- one day at a time
    ) AS day
    WHERE EXTRACT(DOW FROM day) NOT IN (0, 6)
    ) :: INTEGER AS lead_time_in_business_days,
    /*
    the only negative results come from order_qty = -1, likely returns or some S&OP adjustment, which is expected business behavior in my opinion, not a data issue.
    */
    (d.order_qty * (d.unit_price - d.unit_price_discount)) :: NUMERIC(12, 4) AS total_line_extended_price
FROM
    store.store_sales_order_detail d
    INNER JOIN store.store_sales_order_header h ON d.sales_order_id = h.sales_order_id;

GET DIAGNOSTICS v_rows = ROW_COUNT;
RAISE NOTICE '[5/5] publish.publish_orders: loaded with % rows', v_rows;

RAISE NOTICE '---TL DONE---';

END $$;


-- ============================================================================
-- question queries
-- ============================================================================


-- Which color generated the highest revenue each year?
-- I used RANK instead of ROW_NUMBER because I think that if two colors tie in revenue both should show up
WITH revenue_per_year_color AS (
    SELECT
        EXTRACT(YEAR FROM o.order_date) AS year,
		p.color,
        SUM(o.total_line_extended_price) AS revenue
    FROM publish.publish_orders o
    JOIN publish.publish_product p ON o.product_id = p.product_id
    GROUP BY year, p.color
),
ranking AS (
    SELECT
        year,
		color,
		revenue,
        RANK() OVER (PARTITION BY year ORDER BY revenue DESC) AS rk
    FROM revenue_per_year_color
)
SELECT
    year,color,revenue
FROM ranking
WHERE rk = 1
ORDER BY year;



-- What is the average LeadTimeInBusinessDays by ProductCategoryName?
SELECT
    p.product_category_name  AS product_category_name,
    AVG(o.lead_time_in_business_days)          AS avg_lead_time_in_business_days
FROM publish.publish_orders o
INNER JOIN publish.publish_product p
    ON o.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY avg_lead_time_in_business_days DESC;
