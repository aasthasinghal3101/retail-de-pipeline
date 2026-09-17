"""
generate_raw_data.py
---------------------
Simulates the EXTRACT step of an ELT pipeline.

In a production deployment this module would be replaced by real connectors
(e.g. a Fivetran/Airbyte sync, a REST API pull from an OMS, or a Kafka
consumer landing CDC events). For this project it deterministically
generates a realistic e-commerce dataset — customers, products, and orders —
and lands it as CSV files in a "raw" zone (data/raw/), exactly the way raw
data would land in an S3/ADLS bucket before being loaded into the warehouse.

Usage:
    python ingestion/generate_raw_data.py --customers 500 --products 120 --orders 8000
"""
from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

REGIONS = ["North", "South", "East", "West", "Central"]
CATEGORIES = {
    "Electronics": (1500, 85000),
    "Home & Kitchen": (300, 15000),
    "Fashion": (250, 6000),
    "Sports & Outdoors": (400, 12000),
    "Books": (150, 2000),
    "Beauty & Personal Care": (200, 4000),
    "Grocery": (50, 1500),
}
ORDER_STATUSES = ["DELIVERED", "SHIPPED", "PROCESSING", "CANCELLED", "RETURNED"]
ORDER_STATUS_WEIGHTS = [0.70, 0.12, 0.08, 0.06, 0.04]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery"]


def generate_customers(fake: Faker, n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        signup_date = fake.date_time_between(start_date="-3y", end_date="-1d")
        rows.append(
            {
                "customer_id": f"CUST{i:06d}",
                "full_name": fake.name(),
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "region": random.choice(REGIONS),
                "city": fake.city(),
                "signup_ts": signup_date.isoformat(),
                "loyalty_tier": random.choices(
                    ["Bronze", "Silver", "Gold", "Platinum"], weights=[0.5, 0.3, 0.15, 0.05]
                )[0],
                # a few intentionally messy/duplicate rows to make the
                # staging layer's cleaning logic demonstrably necessary
                "_source_system": random.choice(["web_crm", "mobile_crm", "legacy_erp"]),
            }
        )
    df = pd.DataFrame(rows)
    # inject a handful of dirty records: null emails, whitespace, dup ids
    dirty_idx = df.sample(frac=0.02, random_state=42).index
    df.loc[dirty_idx, "email"] = None
    df.loc[df.sample(frac=0.01, random_state=7).index, "full_name"] = df["full_name"] + "   "
    dup_rows = df.sample(frac=0.005, random_state=3)
    df = pd.concat([df, dup_rows], ignore_index=True)
    return df


def generate_products(fake: Faker, n: int) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        category = random.choice(list(CATEGORIES.keys()))
        low, high = CATEGORIES[category]
        rows.append(
            {
                "product_id": f"PROD{i:05d}",
                "product_name": fake.catch_phrase(),
                "category": category,
                "unit_price": round(random.uniform(low, high), 2),
                "supplier": fake.company(),
                "is_active": random.choices([True, False], weights=[0.92, 0.08])[0],
            }
        )
    return pd.DataFrame(rows)


def generate_orders(
    fake: Faker, customers: pd.DataFrame, products: pd.DataFrame, n: int
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (orders, order_items) — a classic header/detail pair,
    mirroring how OLTP order systems actually model data."""
    customer_ids = customers["customer_id"].tolist()
    product_records = products.to_dict("records")

    order_rows = []
    item_rows = []
    start = datetime.now() - timedelta(days=395)  # ~13 months of history

    for i in range(1, n + 1):
        order_id = f"ORD{i:07d}"
        order_ts = start + timedelta(
            seconds=random.randint(0, int(timedelta(days=395).total_seconds()))
        )
        customer_id = random.choice(customer_ids)
        status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0]
        n_items = random.randint(1, 5)
        chosen_products = random.sample(product_records, k=min(n_items, len(product_records)))

        order_total = 0.0
        for line_no, prod in enumerate(chosen_products, start=1):
            qty = random.randint(1, 4)
            unit_price = prod["unit_price"]
            discount_pct = random.choices([0, 5, 10, 15, 20], weights=[0.55, 0.15, 0.15, 0.1, 0.05])[0]
            line_total = round(qty * unit_price * (1 - discount_pct / 100), 2)
            order_total += line_total
            item_rows.append(
                {
                    "order_item_id": f"{order_id}-{line_no}",
                    "order_id": order_id,
                    "product_id": prod["product_id"],
                    "quantity": qty,
                    "unit_price": unit_price,
                    "discount_pct": discount_pct,
                    "line_total": line_total,
                }
            )

        order_rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_ts": order_ts.isoformat(),
                "status": status,
                "payment_method": random.choice(PAYMENT_METHODS),
                "order_total": round(order_total, 2),
            }
        )

    return pd.DataFrame(order_rows), pd.DataFrame(item_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate raw e-commerce source data.")
    parser.add_argument("--customers", type=int, default=500)
    parser.add_argument("--products", type=int, default=120)
    parser.add_argument("--orders", type=int, default=8000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    customers = generate_customers(fake, args.customers)
    products = generate_products(fake, args.products)
    orders, order_items = generate_orders(fake, customers, products, args.orders)

    customers.to_csv(RAW_DIR / "customers.csv", index=False)
    products.to_csv(RAW_DIR / "products.csv", index=False)
    orders.to_csv(RAW_DIR / "orders.csv", index=False)
    order_items.to_csv(RAW_DIR / "order_items.csv", index=False)

    print("Raw data generated:")
    print(f"  customers.csv    {len(customers):>7,} rows")
    print(f"  products.csv     {len(products):>7,} rows")
    print(f"  orders.csv       {len(orders):>7,} rows")
    print(f"  order_items.csv  {len(order_items):>7,} rows")
    print(f"Written to: {RAW_DIR}")


if __name__ == "__main__":
    main()
