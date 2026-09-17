"""Unit tests for the ingestion layer — run with: pytest tests/"""
import sys
from pathlib import Path

import pandas as pd
import pytest
from faker import Faker

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "ingestion"))

from generate_raw_data import (  # noqa: E402
    generate_customers,
    generate_orders,
    generate_products,
)


@pytest.fixture
def fake():
    f = Faker()
    Faker.seed(1)
    return f


def test_generate_customers_row_count(fake):
    df = generate_customers(fake, 100)
    # a small % of duplicate rows are injected deliberately (see docstring
    # in generate_raw_data.py) so count should be >= requested
    assert len(df) >= 100


def test_generate_customers_has_expected_columns(fake):
    df = generate_customers(fake, 10)
    expected = {
        "customer_id", "full_name", "email", "phone", "region",
        "city", "signup_ts", "loyalty_tier", "_source_system",
    }
    assert expected.issubset(set(df.columns))


def test_generate_customers_injects_null_emails(fake):
    # the generator intentionally dirties ~2% of emails to make staging's
    # cleaning logic demonstrably necessary
    df = generate_customers(fake, 500)
    assert df["email"].isna().sum() > 0


def test_generate_products_price_within_category_band(fake):
    df = generate_products(fake, 200)
    assert (df["unit_price"] > 0).all()
    assert df["category"].isin(
        [
            "Electronics", "Home & Kitchen", "Fashion", "Sports & Outdoors",
            "Books", "Beauty & Personal Care", "Grocery",
        ]
    ).all()


def test_generate_orders_referential_integrity(fake):
    customers = generate_customers(fake, 50)
    products = generate_products(fake, 30)
    orders, order_items = generate_orders(fake, customers, products, 100)

    assert set(orders["customer_id"]).issubset(set(customers["customer_id"]))
    assert set(order_items["order_id"]).issubset(set(orders["order_id"]))
    assert set(order_items["product_id"]).issubset(set(products["product_id"]))


def test_generate_orders_line_totals_are_positive(fake):
    customers = generate_customers(fake, 50)
    products = generate_products(fake, 30)
    _, order_items = generate_orders(fake, customers, products, 100)
    assert (order_items["line_total"] >= 0).all()


def test_generate_orders_every_order_has_at_least_one_item(fake):
    customers = generate_customers(fake, 50)
    products = generate_products(fake, 30)
    orders, order_items = generate_orders(fake, customers, products, 50)
    orders_with_items = set(order_items["order_id"])
    assert set(orders["order_id"]).issubset(orders_with_items)
