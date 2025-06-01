CREATE SCHEMA IF NOT EXISTS star_schema;

-----------------------
-- Dimension Tables
-----------------------

-- Клиенты
CREATE TABLE IF NOT EXISTS star_schema.dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     BIGINT,
    first_name      TEXT,
    last_name       TEXT,
    age             INTEGER,
    email           TEXT,
    country         TEXT,
    postal_code     TEXT,
    pet_type        TEXT,
    pet_name        TEXT,
    pet_breed       TEXT
);

-- Продавцы
CREATE TABLE IF NOT EXISTS star_schema.dim_seller (
    seller_key    SERIAL PRIMARY KEY,
    seller_id     BIGINT,
    first_name    TEXT,
    last_name     TEXT,
    email         TEXT,
    country       TEXT,
    postal_code   TEXT
);

-- Продукты
CREATE TABLE IF NOT EXISTS star_schema.dim_product (
    product_key          SERIAL PRIMARY KEY,
    product_id           BIGINT,
    name                 TEXT,
    category             TEXT,
    price                NUMERIC(10,2),
    weight               NUMERIC(10,2),
    color                TEXT,
    size                 TEXT,
    brand                TEXT,
    material             TEXT,
    rating               NUMERIC(3,1),
    reviews              INTEGER,
    pet_category         TEXT,
    product_description  TEXT,
    product_release_date DATE,
    product_expiry_date  DATE,
    supplier_name        TEXT
);

-- Магазины
CREATE TABLE IF NOT EXISTS star_schema.dim_store (
    store_key   SERIAL PRIMARY KEY,
    store_name  TEXT,
    location    TEXT,
    city        TEXT,
    state       TEXT,
    country     TEXT,
    phone       TEXT,
    email       TEXT
);

-- Поставщики
CREATE TABLE IF NOT EXISTS star_schema.dim_supplier (
    supplier_key    SERIAL PRIMARY KEY,
    supplier_name   TEXT,
    contact         TEXT,
    email           TEXT,
    phone           TEXT,
    address         TEXT,
    city            TEXT,
    country         TEXT
);

-- Даты продаж
CREATE TABLE IF NOT EXISTS star_schema.dim_date (
    date_key    SERIAL PRIMARY KEY,
    sale_date   DATE,
    year        INTEGER,
    month       INTEGER,
    day         INTEGER
);

-----------------------
-- Fact Table
-----------------------

CREATE TABLE IF NOT EXISTS star_schema.fact_sales (
    sale_id          BIGINT PRIMARY KEY,
    customer_key     INTEGER REFERENCES star_schema.dim_customer(customer_key),
    seller_key       INTEGER REFERENCES star_schema.dim_seller(seller_key),
    product_key      INTEGER REFERENCES star_schema.dim_product(product_key),
    store_key        INTEGER REFERENCES star_schema.dim_store(store_key),
    supplier_key     INTEGER REFERENCES star_schema.dim_supplier(supplier_key),
    date_key         INTEGER REFERENCES star_schema.dim_date(date_key),
    sale_quantity    INTEGER,
    sale_total_price NUMERIC(10,2)
);
