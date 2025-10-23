# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
from __future__ import annotations
import argparse
import csv
import json
import math
import pathlib
import random
import rstr
from collections import defaultdict
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xlsxwriter
from faker import Faker

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", type=str, default="data_raw")
    ap.add_argument(
        "--scale",
        type=float,
        default=0.05,
        help="Scale factor for row counts. 1.0 ~ full, 0.05 small sample",
    )
    return ap.parse_args()

def ensure_dir(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)

AU_LAT_RANGE = (-44.0, -10.0)
AU_LON_RANGE = (112.0, 154.0)
def au_latlon():
    return (round(np.random.uniform(*AU_LAT_RANGE), 6),
            round(np.random.uniform(*AU_LON_RANGE), 6))

def dt_range(start: date, days: int):
    for i in range(days):
        yield start + timedelta(days=i)

def main():
    args = parse_args()
    random.seed(args.seed); np.random.seed(args.seed)
    out = pathlib.Path(args.out); ensure_dir(out)

    # Row counts at scale=1.0 (doc-sized). Scaled down for samples.
    n_customers = max(200, int(80000 * args.scale))
    n_products = max(200, int(25000 * args.scale))
    n_stores = max(20, int(5000 * args.scale))
    n_suppliers = max(50, int(8000 * args.scale))
    n_orders = max(2000, int(1000000 * args.scale))
    n_events = max(2000, int(2000000 * args.scale))
    n_returns = max(2000, int(100000 * args.scale))
    n_shipments = max(5000, int(1000000 * args.scale))
    days_span = max(14, int(1100 * args.scale))  # orders/events horizon
    start_day = date.today() - timedelta(days=days_span)

    # Minimal sample generation (expand to full volumes per docs)
    fake = Faker('en_AU')
    customers_path = out/'customers.csv'
    with customers_path.open('w', encoding='utf-8') as f:
        f.write('customer_id,natural_key,first_name,last_name,email,phone,address_line1,address_line2,city,state_region,postcode,country_code,latitude,longitude,birth_date,join_ts,is_vip,gdpr_consent\n')
        for i in range(1, n_customers):  # TODO raise to 80_000
            nk = 'CUST-' + rstr.rstr('A-Z0-9', 8)
            email = fake.email() if random.random()>0.008 else 'bad_email'
            lat = -44 + random.random()*10; lon = 112 + random.random()*40
            birth = date(1960,1,1) + timedelta(days=random.randint(0, 20000))
            join_ts = datetime(2024,1,1) + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86399))
            phone = None if random.random() < 0.015 else fake.phone_number().replace(',',' ')
            addr1 = None if random.random() < 0.01 else fake.street_address().replace(',',' ')
            addr2 = ""
            f.write(f"{i},{nk},{fake.first_name()},{fake.last_name()},{email},{phone},{addr1},{addr2},{fake.city().replace(',',' ')},{fake.state_abbr()},{fake.postcode()},AU,{lat:.6f},{lon:.6f},{birth.isoformat()},{join_ts.isoformat()},{str(random.random()<0.15)},{str(random.random()>0.05)}\n")

    # ~0.2% duplicate natural_key rows (re-using NK but new id)
    dups = max(1, int(0.002 * n_customers))
    with customers_path.open('a', encoding='utf-8') as f:
        for _ in range(dups):
            src = random.randint(1, n_customers)
            i = n_customers + _ + 1
            nk = f"CUST-{src:07d}"
            email = fake.email()  # keep valid here
            lat = -44 + random.random()*10; lon = 112 + random.random()*40
            birth = date(1960,1,1) + timedelta(days=random.randint(0, 20000))
            join_ts = datetime(2024,1,1) + timedelta(days=random.randint(0, 400),
                                                    seconds=random.randint(0, 86399))
            phone = None if random.random() < 0.015 else fake.phone_number().replace(',',' ')
            addr1 = None if random.random() < 0.01 else fake.street_address().replace(',',' ')
            addr2 = ""
            f.write(f"{i},{nk},{fake.first_name()},{fake.last_name()},{email},{phone},{addr1},{addr2},"
                    f"{fake.city().replace(',',' ')},{fake.state_abbr()},{fake.postcode()},AU,"
                    f"{lat:.6f},{lon:.6f},{birth.isoformat()},{join_ts.isoformat()},False,True\n")

    #Shipment
    ship_ids        = list(range(1, n_shipments + 1))
    order_ids_for_ship = [random.randint(1, max(1, n_orders)) for _ in range(n_shipments)]
    carriers_list   = ['AUSPOST'] * n_shipments
    cost_cents      = [1995] * n_shipments 
    shipped_list, delivered_list = [], []
    for _ in range(n_shipments):
        s = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 90))
        shipped_list.append(s)
        if random.random() < 0.05:
            delivered_list.append(None)  # in transit (NULL)
        else:
            lag = random.randint(0, 6)
            if random.random() < 0.02:
                lag += random.randint(7, 21)  # late beyond SLA
            delivered_list.append(s + timedelta(days=lag)) 

    tbl = pa.table({
        'shipment_id':  pa.array(ship_ids, type=pa.int64()),
        'order_id':     pa.array(order_ids_for_ship, type=pa.int64()),
        'carrier':      pa.array(carriers_list, type=pa.string()),
        'shipped_at':   pa.array(shipped_list, type=pa.timestamp('us')),
        'delivered_at': pa.array(delivered_list, type=pa.timestamp('us')),
        'ship_cost':    pa.array(cost_cents, type=pa.int64()).cast(pa.decimal128(21, 2)),
    })
    pq.write_table(tbl, out / 'shipments.parquet', compression='snappy')

    # Product
    products_path = out/'products.csv'
    with products_path.open('w', encoding='utf-8') as f:
        f.write('product_id,sku,name,category,subcategory,current_price,currency,is_discontinued,introduced_dt,discontinued_dt\n')
        cats = ["Electronics","Home","Grocery","Clothing","Beauty","Toys"]
        subs = {
            "Electronics":["Phones","Audio","TV","Computers"],
            "Home":["Kitchen","Bedding","Bath"],
            "Grocery":["Snacks","Beverages","Pantry"],
            "Clothing":["Men","Women","Kids"],
            "Beauty":["Skin","Hair","Makeup"],
            "Toys":["Outdoor","STEM","Board"]
        }
        for pid in range(1, n_products):
            cat = random.choice(cats); sub = random.choice(subs[cat])
            price = round(np.random.lognormal(2.4, 0.5), 4)  if random.random() > 0.003 else ""
            intro = date(2021,1,1) + timedelta(days=random.randint(0, 1200))
            discontinued = '' if random.random()<0.9 else (intro + timedelta(days=random.randint(60, 800))).isoformat()
            is_disc = random.random() < 0.08
            discontinued = "" if (is_disc and random.random() < 0.5) else discontinued
            f.write(f"{pid},SKU-{pid:07d},{fake.word().title()},{cat},{sub},{price},AUD,{str(random.random()<0.08)},{intro.isoformat()},{discontinued}\n")

    # Store
    stores_path = out/'stores.csv'
    with stores_path.open('w', encoding='utf-8') as f:
        f.write('store_id,store_code,name,channel,region,state,latitude,longitude,open_dt,close_dt\n')
        channels = ['Retail','Online','Franchise']
        regions = ['ANZ','NSW','SA','VIC','QLD','WA','TAS','NT','ACT']
        for sid in range(1, n_stores):
            lat, lon = au_latlon()
            # 0.5% impossible coordinates
            if random.random() < 0.005:
                lat, lon = 999.0, 999.0
            code = f"STR-{sid:05d}"
            # 0.2% duplicate store_code (re-use a previous sid's code)
            if sid > 10 and random.random() < 0.002:
                code = f"STR-{random.randint(1, sid-1):05d}"
            open_dt = date(2012,1,1) + timedelta(days=random.randint(0, 3650))
            close_dt = '' if random.random()<0.95 else (open_dt + timedelta(days=random.randint(365, 3650))).isoformat()
            f.write(f"{sid},{code},{fake.company()},{random.choice(channels)},{random.choice(regions)},{fake.state_abbr()},{lat},{lon},{open_dt.isoformat()},{close_dt}\n")

    # Suppliers 
    suppliers_path = out/'suppliers.csv'
    with suppliers_path.open('w', encoding='utf-8') as f:
        f.write('supplier_id,supplier_code,name,country_code,lead_time_days,preferred\n')
        for sp in range(1, n_suppliers):
            lead = int(np.clip(np.random.normal(10, 4), 1, 30))
            f.write(f"{sp},SUP-{sp:05d},{fake.company()},AU,{lead},{str(random.random()<0.2)}\n")

    # exchange_rates XLSX  
    xr_dir = out / "exchange_rates"
    ensure_dir(xr_dir)
    wb_path = xr_dir / "exchange_rates.xlsx"
    wb = xlsxwriter.Workbook(str(wb_path))
    ws = wb.add_worksheet("rates")
    headers = ["date", "currency", "rate_to_aud"]
    for c, h in enumerate(headers):
        ws.write(0, c, h)
    currencies = ["AUD", "USD", "EUR", "JPY", "GBP", "NZD"]
    # ~3 years at scale 1.0; min 30 days for small scale
    rate_days = max(30, int(1095 * args.scale))
    start_rates = date.today() - timedelta(days=rate_days)
    r = 1
    base_map = {"AUD": 1.0, "USD": 1.47, "EUR": 1.60, "JPY": 0.0096, "GBP": 1.86, "NZD": 0.92}
    for d in dt_range(start_rates, rate_days):
        for cur in currencies:
            rate = round(base_map[cur] * (1 + np.random.normal(0, 0.01)), 8)
            ws.write(r, 0, d.isoformat())
            ws.write(r, 1, cur)
            ws.write_number(r, 2, rate)
            r += 1
    wb.close()

    # Orders and Order Line
    orders_dir = out / "orders_header"
    lines_dir = out / "orders_lines"
    ensure_dir(orders_dir)
    ensure_dir(lines_dir)

    customer_ids = list(range(1, n_customers + 1))
    store_ids = list(range(1, n_stores + 1))
    product_ids = list(range(1, n_products + 1))

    daily_orders = max(100, int(n_orders / days_span))
    order_id = 1
    dupe_p = 0.0005  # 0.05% duplicate order_id across days
    reuse_pool: list[int] = []

    for d in dt_range(start_day, days_span):
        # build today's orders and lines, then write both CSVs for this date
        orders_rows = []
        lines_rows = []
        for _ in range(daily_orders):
            # duplicate cross-file sometimes
            if reuse_pool and random.random() < dupe_p:
                oid = random.choice(reuse_pool)
            else:
                oid = order_id
                order_id += 1
                if random.random() < 0.05:
                    reuse_pool.append(oid)

            cid = random.choice(customer_ids) if random.random() > 0.01 else 9_999_999_999  # 1% FK violation
            sid = random.choice(store_ids) if random.random() > 0.01 else 9_999_999_998     # 1% FK violation
            ts = datetime(d.year, d.month, d.day, random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
            pm = random.choice(["CARD", "CASH", "PAYPAL", "AFTERPAY"])
            coupon = "" if random.random() < 0.85 else ("COUPON" + str(random.randint(100, 999)))
            ship_fee = round(max(0, np.random.normal(12.0, 5.0)), 2)
            currency = "AUD"
            orders_rows.append(
                [
                    oid,
                    ts.isoformat(),
                    d.isoformat(),
                    cid,
                    sid,
                    "Retail",
                    pm,
                    coupon,
                    f"{ship_fee:.2f}",
                    currency,
                ]
            )

            # lines for this order (2..5)
            n_lines = random.randint(2, 5)
            for ln in range(1, n_lines + 1):
                pid = 9_999_999_997 if random.random() < 0.01 else random.choice(product_ids)  # 1% invalid PID
                qty = random.randint(1, 5)
                if random.random() < 0.002:
                    qty = -qty  # rare negative
                if random.random() < 0.001:
                    qty = 0     # rare zero qty
                unit_price = round(max(0.5, np.random.lognormal(2.2, 0.5)), 4)
                if random.random() < 0.001:
                    unit_price = 0.0  # rare free
                disc = round(max(0, min(0.8, np.random.beta(2, 10))), 4)
                tax = round(max(0, min(0.3, np.random.beta(2, 20))), 4)
                lines_rows.append([oid, ln, pid, qty, f"{unit_price:.4f}", f"{disc:.4f}", f"{tax:.4f}"])

        # write partitions for this date
        day_dir = orders_dir / f"dt={d.isoformat()}"
        ensure_dir(day_dir)
        header_path = day_dir / "part-00001.csv"
        with header_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "order_id",
                    "order_ts",
                    "order_dt_local",
                    "customer_id",
                    "store_id",
                    "channel",
                    "payment_method",
                    "coupon_code",
                    "shipping_fee",
                    "currency",
                ]
            )
            w.writerows(orders_rows)

        line_dir = lines_dir / f"dt={d.isoformat()}"
        ensure_dir(line_dir)
        lines_path = line_dir / "part-00001.csv"
        with lines_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "order_id",
                    "line_number",
                    "product_id",
                    "qty",
                    "unit_price",
                    "line_discount_pct",
                    "tax_pct",
                ]
            )
            w.writerows(lines_rows)

    # EVENTS 

    events_root = out / "events"
    ensure_dir(events_root)
    bad_rate = 0.0005  # 0.05% malformed
    total_events = n_events
    # bucket lines by date first to minimize open/close
    buckets: dict[str, list[str]] = defaultdict(list)
    for i in range(total_events):
        ts = datetime.utcnow() - timedelta(seconds=random.randint(0, 86400 * min(30, days_span)))
        day = ts.date().isoformat()
        if random.random() < bad_rate:
            buckets[day].append("{bad_json_line\n")
            continue
        payload = {
            "event_id": i + 1,
            "event_ts": ts.isoformat(),
            "event_type": random.choice(["click", "view", "cart_add", "purchase", "search"]),
            "user_id": random.choice(customer_ids) if random.random() > 0.02 else None,
            "session_id": "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=16)),
            "metadata": {"ua": fake.user_agent(), "ip": fake.ipv4()},
        }
        # drop a required field sometimes
        if random.random() < 0.005:
            payload.pop("session_id", None)
        buckets[day].append(json.dumps(payload) + "\n")

    for day, lines in buckets.items():
        part_dir = events_root / f"event_dt={day}"
        ensure_dir(part_dir)
        with (part_dir / "events_0001.jsonl").open("w", encoding="utf-8") as f:
            f.writelines(lines)

    # SENSORS 

    sensors_root = out / "sensors"
    ensure_dir(sensors_root)
    # build months across the span
    months = sorted({(start_day + timedelta(days=i)).strftime("%Y-%m") for i in range(days_span)})
    # generate for a subset of stores if large
    store_sample = store_ids if len(store_ids) <= 50 else random.sample(store_ids, 50)

    for sid in store_sample:
        for m in months:
            rows = []
            # ~ 28 readings/month (1 per day for sample)
            for day in range(1, 29):
                ts = datetime.fromisoformat(f"{m}-{day:02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00")
                t_c = round(np.random.normal(22, 5), 2)
                h_p = round(np.random.normal(55, 10), 2)
                # ~0.3% out-of-range each
                if random.random() < 0.003:
                    t_c = float(random.choice([-50, 85, 120]))
                if random.random() < 0.003:
                    h_p = float(random.choice([-5, 120]))
                battery = int(np.clip(int(np.random.normal(3700, 150)), 3000, 4200))
                sensor_ts = "" if random.random() < 0.002 else ts.isoformat()
                rows.append([sensor_ts, sid, f"SHELF-{random.randint(1,50):03d}", f"{t_c:.2f}", f"{h_p:.2f}", battery])

            part_dir = sensors_root / f"store_id={sid}" / f"month={m}"
            ensure_dir(part_dir)
            with (part_dir / "part-00001.csv").open("w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)
                w.writerow(["sensor_ts", "store_id", "shelf_id", "temperature_c", "humidity_pct", "battery_mv"])
                w.writerows(rows)


    # RETURNS DAY1 

    returns_dir = out / "returns_day1"
    ensure_dir(returns_dir)
    df_ret = pd.DataFrame(
        {
            "return_id": np.arange(1, n_returns + 1, dtype=np.int64),
            "order_id": np.random.randint(1, max(2, n_orders), size=n_returns, dtype=np.int64),
            "product_id": np.random.randint(1, max(2, n_products), size=n_returns, dtype=np.int64),
            "return_ts": [datetime.utcnow() - timedelta(days=int(x)) for x in np.random.randint(0, 90, size=n_returns)],
            "qty": np.random.choice([1, 1, 1, 2], size=n_returns).astype(np.int32),
            "reason": np.random.choice(["Damaged", "Wrong Item", "Changed Mind", "Other"], size=n_returns),
        }
    )
    
    df_ret.to_parquet(returns_dir / "part-00001.parquet", index=False)

    print(f"✅ Sample raw written to {out}. Expand to required volumes per /docs.")

if __name__ == '__main__':
    main()

