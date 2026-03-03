DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS store CASCADE;
DROP SCHEMA IF EXISTS publish CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA store;
CREATE SCHEMA publish;


CREATE TABLE raw.raw_products (
    "ProductID"              TEXT,
    "ProductDesc"            TEXT,
    "ProductNumber"          TEXT,
    "MakeFlag"               TEXT,
    "Color"                  TEXT,
    "SafetyStockLevel"       TEXT,
    "ReorderPoint"           TEXT,
    "StandardCost"           TEXT,
    "ListPrice"              TEXT,
    "Size"                   TEXT,
    "SizeUnitMeasureCode"    TEXT,
    "Weight"                 TEXT,
    "WeightUnitMeasureCode"  TEXT,
    "ProductCategoryName"    TEXT,
    "ProductSubCategoryName" TEXT
);

CREATE TABLE raw.raw_sales_order_detail (
    "SalesOrderID"       TEXT,
    "SalesOrderDetailID" TEXT,
    "OrderQty"           TEXT,
    "ProductID"          TEXT,
    "UnitPrice"          TEXT,
    "UnitPriceDiscount"  TEXT
);

CREATE TABLE raw.raw_sales_order_header (
    "SalesOrderID"    TEXT,
    "OrderDate"       TEXT,
    "ShipDate"        TEXT,
    "OnlineOrderFlag" TEXT,
    "AccountNumber"   TEXT,
    "CustomerID"      TEXT,
    "SalesPersonID"   TEXT,
    "Freight"         TEXT
);

\copy raw.raw_products FROM 'C:\upstart13\products-1-.csv' WITH (FORMAT csv, HEADER true, QUOTE '"', DELIMITER ',', ENCODING 'UTF8');

\copy raw.raw_sales_order_detail FROM 'C:\upstart13\sales-order-detail-1-.csv' WITH (FORMAT csv, HEADER true, QUOTE '"', DELIMITER ',', ENCODING 'UTF8');

\copy raw.raw_sales_order_header FROM 'C:\upstart13\sales-order-header-1-.csv' WITH (FORMAT csv, HEADER true, QUOTE '"', DELIMITER ',', ENCODING 'UTF8');
