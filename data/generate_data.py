#!/usr/bin/env python3
"""Create realistic sample source files for the demo pipelines.

Run once from the project root:
    python data/generate_data.py

Files created:
    data/sales_2026_10_07.csv      150 orders
    data/sales_2026_10_08.csv      145 new orders + 5 duplicate order ids + 2 bad rows
    data/clickstream_2026_10_08.json  300 events, 30 intentionally bad
    data/customers.csv             200 customers
    data/customers_update.csv      40 changed + 20 new customers (merge demo)
"""
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(7)
DATA = Path(__file__).parent

REGIONS = ["North", "South", "East", "West"]
PRODUCTS = [
    ("Laptop Pro 14", "Electronics", 899.00),
    ("Nimbus Headphones", "Electronics", 149.00),
    ("Trailpack 40L", "Outdoors", 79.00),
    ("Everpour Bottle", "Outdoors", 29.00),
    ("Lumen Desk Lamp", "Home", 59.00),
    ("Cloudrest Pillow", "Home", 49.00),
    ("Velocity Runners", "Apparel", 129.00),
    ("Atlas Denim Jacket", "Apparel", 119.00),
]
FIRST = ["Aarav", "Maya", "Liam", "Sofia", "Noah", "Priya", "Ethan", "Zara",
         "Kiran", "Nora", "Ravi", "Elena", "Dev", "Anika"]
LAST = ["Sharma", "Patel", "Garcia", "Kim", "Nguyen", "Khan", "Murphy", "Ali",
        "Das", "Rossi", "Chen", "Iyer", "Rao", "Silva"]
EVENT_TYPES = ["view", "click", "purchase"]
PAGES = ["/home", "/products", "/products/laptop-pro-14", "/cart", "/checkout", "/deals"]
TIERS = ["basic", "standard", "premium"]


def write_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows -> {path}")


def sales_rows(order_ids, sale_date, customer_pool):
    rows = []
    for oid in order_ids:
        product, category, price = random.choice(PRODUCTS)
        rows.append([
            oid,
            random.choice(customer_pool),
            sale_date,
            product,
            category,
            random.choice(REGIONS),
            random.randint(1, 4),
            f"{price:.2f}",
        ])
    return rows


def make_sales():
    header = ["orderId", "customer_id", "sale_date", "product", "category",
              "region", "quantity", "unitPrice"]
    customers = list(range(1, 201))
    file1_ids = list(range(10001, 10151))
    file2_ids = list(range(10151, 10296))
    dup_ids = [10003, 10027, 10059, 10088, 10112]
    write_csv(DATA / "sales_2026_10_07.csv", header,
              sales_rows(file1_ids, "2026-10-07", customers))
    rows2 = sales_rows(file2_ids, "2026-10-08", customers)
    rows2 += sales_rows(dup_ids, "2026-10-08", customers)
    rows2.append([10301, 55, "2026-10-08", "Lumen Desk Lamp", "Home", "North", 0, "59.00"])
    rows2.append([10302, 71, "2026-10-08", "Trailpack 40L", "Outdoors", "South", 0, "79.00"])
    write_csv(DATA / "sales_2026_10_08.csv", header, rows2)


def make_clickstream():
    events = []
    base = datetime(2026, 10, 8, 0, 5, 0)
    for i in range(1, 301):
        ts = base + timedelta(minutes=random.randint(0, 1400), seconds=random.randint(0, 59))
        events.append({
            "event_id": f"evt-{i:05d}",
            "event_time": ts.strftime("%Y-%m-%dT%H:%M:%S"),
            "user_id": random.randint(1, 200),
            "event_type": random.choice(EVENT_TYPES),
            "page": random.choice(PAGES),
            "session_id": f"sess-{random.randint(1000, 9999)}",
        })
    for i in range(10):
        events[i]["event_id"] = None
    for i in range(10, 20):
        events[i]["event_type"] = "hover"
    for i in range(20, 30):
        events[i]["user_id"] = None
    path = DATA / "clickstream_2026_10_08.json"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(events, fh, indent=1)
    print(f"wrote {len(events)} events (30 intentionally bad) -> {path}")


def customer_row(cid):
    first = random.choice(FIRST)
    last = random.choice(LAST)
    signup = (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d")
    return {
        "customer_id": cid,
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{cid}@example.com",
        "region": random.choice(REGIONS),
        "signup_date": signup,
        "tier": random.choice(TIERS),
    }


def make_customers():
    header = ["customer_id", "name", "email", "region", "signup_date", "tier"]
    base = [customer_row(cid) for cid in range(1, 201)]
    write_csv(DATA / "customers.csv", header,
              [[c[h] for h in header] for c in base])
    updates = []
    for c in base[:40]:
        changed = dict(c)
        tiers = [t for t in TIERS if t != c["tier"]]
        changed["tier"] = tiers[0]
        if c["customer_id"] % 3 == 0:
            others = [r for r in REGIONS if r != c["region"]]
            changed["region"] = others[0]
        updates.append(changed)
    for cid in range(201, 221):
        updates.append(customer_row(cid))
    write_csv(DATA / "customers_update.csv", header,
              [[c[h] for h in header] for c in updates])


if __name__ == "__main__":
    make_sales()
    make_clickstream()
    make_customers()
