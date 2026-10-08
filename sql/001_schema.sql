-- Simulated Olist source system (OLTP).
-- Tables are unqualified: db.apply_schema() creates the configured schema and
-- sets search_path before running this file. Every statement is idempotent.
-- Column names are kept exactly as in the source CSVs (typos included).

-- Reference data: loaded once by scripts/seed_reference.py, insert-only.

CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name         text PRIMARY KEY,
    product_category_name_english text
);

-- No FK to the translation table: two categories (and the blank one) have no translation.
CREATE TABLE IF NOT EXISTS products (
    product_id                 text PRIMARY KEY,
    product_category_name      text,
    product_name_lenght        integer,
    product_description_lenght integer,
    product_photos_qty         integer,
    product_weight_g           integer,
    product_length_cm          integer,
    product_height_cm          integer,
    product_width_cm           integer
);

CREATE TABLE IF NOT EXISTS sellers (
    seller_id              text PRIMARY KEY,
    seller_zip_code_prefix text,
    seller_city            text,
    seller_state           text
);

-- Many rows per zip prefix and ~262k exact duplicates, so there is no natural key.
-- geolocation_row_id is the 1-based data row number in the CSV.
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_row_id          bigint PRIMARY KEY,
    geolocation_zip_code_prefix text,
    geolocation_lat             double precision,
    geolocation_lng             double precision,
    geolocation_city            text,
    geolocation_state           text
);
CREATE INDEX IF NOT EXISTS geolocation_zip_idx ON geolocation (geolocation_zip_code_prefix);

-- Transactional data: written day by day by scripts/replay.py.
-- updated_at is the logical end of the replay day that last changed the row.

-- customer_id is per order; customer_unique_id identifies the person.
CREATE TABLE IF NOT EXISTS customers (
    customer_id              text PRIMARY KEY,
    customer_unique_id       text NOT NULL,
    customer_zip_code_prefix text,
    customer_city            text,
    customer_state           text,
    updated_at               timestamp NOT NULL
);
CREATE INDEX IF NOT EXISTS customers_updated_at_idx ON customers (updated_at);
CREATE INDEX IF NOT EXISTS customers_unique_id_idx ON customers (customer_unique_id);

CREATE TABLE IF NOT EXISTS orders (
    order_id                      text PRIMARY KEY,
    customer_id                   text NOT NULL REFERENCES customers,
    order_status                  text NOT NULL,
    order_purchase_timestamp      timestamp NOT NULL,
    order_approved_at             timestamp,
    order_delivered_carrier_date  timestamp,
    order_delivered_customer_date timestamp,
    order_estimated_delivery_date timestamp,
    updated_at                    timestamp NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_updated_at_idx ON orders (updated_at);

CREATE TABLE IF NOT EXISTS order_items (
    order_id            text NOT NULL REFERENCES orders,
    order_item_id       integer NOT NULL,
    product_id          text NOT NULL REFERENCES products,
    seller_id           text NOT NULL REFERENCES sellers,
    shipping_limit_date timestamp,
    price               numeric(10,2),
    freight_value       numeric(10,2),
    updated_at          timestamp NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);
CREATE INDEX IF NOT EXISTS order_items_updated_at_idx ON order_items (updated_at);

CREATE TABLE IF NOT EXISTS order_payments (
    order_id             text NOT NULL REFERENCES orders,
    payment_sequential   integer NOT NULL,
    payment_type         text,
    payment_installments integer,
    payment_value        numeric(10,2),
    updated_at           timestamp NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);
CREATE INDEX IF NOT EXISTS order_payments_updated_at_idx ON order_payments (updated_at);

-- review_id is not unique on its own: the same review can cover several orders.
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id               text NOT NULL,
    order_id                text NOT NULL REFERENCES orders,
    review_score            smallint,
    review_comment_title    text,
    review_comment_message  text,
    review_creation_date    timestamp,
    review_answer_timestamp timestamp,
    updated_at              timestamp NOT NULL,
    PRIMARY KEY (review_id, order_id)
);
CREATE INDEX IF NOT EXISTS order_reviews_updated_at_idx ON order_reviews (updated_at);
