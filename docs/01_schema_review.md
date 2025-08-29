# Exercise 1 — Raw Schemas Review

## Overview
- Total sources: 11
- Purpose: confirm keys, relationships, types, and downstream expectations before generating data.

## Table keys
- **customers**: PK `customer_id`; NK `natural_key`.
- **products**: PK `product_id`; NK `sku`.
- **stores**: PK `store_id`; NK `store_code`.
- **suppliers**: PK `supplier_id`; NK `supplier_code`.
- **orders_header**: PK `order_id`; FKs `customer_id`, `store_id`.
- **orders_lines**: composite key (`order_id`, `line_number`); FK `product_id`.
- **events**: single column `json` .
- **sensors**: PK `sensor_ts` (timestamp); FKs `store_id`, `shelf_id`.
- **exchange_rates**: PK `date`.
- **shipments**: PK `shipment_id`; FK `order_id`.
- **returns_day1**: PK `return_id`; FKs `order_id`, `product_id`.

## Relationships (high level)
- customers 1—* orders_header (by `customer_id`)
- stores 1—* orders_header (by `store_id`)
- orders_header 1—* orders_lines (by `order_id`)
- products 1—* orders_lines (by `product_id`)
- orders_header 1—* shipments (by `order_id`)
- orders_header 1—* returns (by `order_id`)
- stores 1—* sensors (by `store_id`)

## Formats & partitioning to use later
- **orders_header**: partition by `order_dt_local` → `dt=YYYY-MM-DD`
- **shipment**: partition by `shipped_at` → `dt=YYYY-MM-DD`
- **sensors**: partition by `store_id` and `month=YYYY-MM`

