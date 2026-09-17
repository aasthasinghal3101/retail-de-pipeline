"""
query_library.py
-----------------
The curated knowledge base the RAG assistant retrieves from: a set of
business questions data analysts actually ask, each paired with a
parameterized SQL template that runs against the dbt marts.

This is deliberately the "single source of truth" for what the assistant
can answer — in a production version this file would be generated from
(or synced with) the metric definitions already documented in
dbt_project/models/marts/schema.yml, so the assistant can never drift from
what the warehouse actually means by "revenue".
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class QueryTemplate:
    id: str
    description: str
    # Natural-language phrasings a user might type — this is what gets
    # embedded/retrieved against, not the SQL itself.
    example_questions: list[str]
    sql: str
    answer_template: str  # Python .format() template filled from the SQL result


QUERY_LIBRARY: list[QueryTemplate] = [
    QueryTemplate(
        id="revenue_by_region",
        description="Total revenue broken down by region",
        example_questions=[
            "what is total revenue by region",
            "show me sales by region",
            "which region makes the most money",
            "revenue breakdown per region",
        ],
        sql="""
            select region, round(sum(total_revenue), 2) as revenue
            from main_marts.mart_daily_sales
            group by region
            order by revenue desc
        """,
        answer_template="Revenue by region:\n{rows}",
    ),
    QueryTemplate(
        id="revenue_by_category",
        description="Total revenue broken down by product category",
        example_questions=[
            "what is total revenue by category",
            "which product category sells the most",
            "sales by category",
            "top categories by revenue",
        ],
        sql="""
            select category, round(sum(total_revenue), 2) as revenue
            from main_marts.mart_daily_sales
            group by category
            order by revenue desc
        """,
        answer_template="Revenue by category:\n{rows}",
    ),
    QueryTemplate(
        id="top_customers_ltv",
        description="Top N customers ranked by lifetime spend",
        example_questions=[
            "who are our top customers",
            "highest lifetime value customers",
            "top 10 customers by spend",
            "which customers spend the most",
        ],
        sql="""
            select customer_name, region, loyalty_tier, lifetime_spend
            from main_marts.mart_customer_ltv
            order by lifetime_spend desc
            limit {n}
        """,
        answer_template="Top {n} customers by lifetime spend:\n{rows}",
    ),
    QueryTemplate(
        id="avg_order_value",
        description="Overall average order value",
        example_questions=[
            "what is the average order value",
            "average order size",
            "how much do customers spend per order on average",
        ],
        sql="""
            select round(avg(order_total), 2) as avg_order_value
            from main_staging.stg_orders
            where order_status != 'CANCELLED'
        """,
        answer_template="Average order value: ₹{rows}",
    ),
    QueryTemplate(
        id="orders_by_status",
        description="Order count broken down by status",
        example_questions=[
            "how many orders are cancelled",
            "order status breakdown",
            "how many orders were delivered vs returned",
            "order counts by status",
        ],
        sql="""
            select order_status, count(*) as order_count
            from main_staging.stg_orders
            group by order_status
            order by order_count desc
        """,
        answer_template="Orders by status:\n{rows}",
    ),
    QueryTemplate(
        id="customers_at_risk",
        description="Customers who haven't ordered recently (churn risk)",
        example_questions=[
            "which customers are at risk of churning",
            "customers who haven't ordered recently",
            "inactive customers",
            "who hasn't purchased in a while",
        ],
        sql="""
            select customer_name, region, loyalty_tier, days_since_last_order
            from main_marts.mart_customer_ltv
            where days_since_last_order > 90
            order by days_since_last_order desc
            limit {n}
        """,
        answer_template="{n} customers with no order in 90+ days (highest churn risk):\n{rows}",
    ),
    QueryTemplate(
        id="revenue_by_payment_method",
        description="Total revenue broken down by payment method",
        example_questions=[
            "revenue by payment method",
            "which payment method is most popular",
            "how much do people pay with UPI vs card",
        ],
        sql="""
            select payment_method, round(sum(order_total), 2) as revenue
            from main_staging.stg_orders
            where order_status != 'CANCELLED'
            group by payment_method
            order by revenue desc
        """,
        answer_template="Revenue by payment method:\n{rows}",
    ),
    QueryTemplate(
        id="loyalty_tier_counts",
        description="Customer count by loyalty tier",
        example_questions=[
            "how many customers are in each loyalty tier",
            "loyalty tier breakdown",
            "how many gold or platinum members do we have",
        ],
        sql="""
            select loyalty_tier, count(*) as customer_count
            from main_marts.dim_customers
            group by loyalty_tier
            order by customer_count desc
        """,
        answer_template="Customers by loyalty tier:\n{rows}",
    ),
    QueryTemplate(
        id="daily_sales_trend",
        description="Recent daily revenue trend",
        example_questions=[
            "show me the daily sales trend",
            "how has revenue changed over the last few days",
            "recent daily revenue",
        ],
        sql="""
            select order_date, round(sum(total_revenue), 2) as revenue
            from main_marts.mart_daily_sales
            group by order_date
            order by order_date desc
            limit {n}
        """,
        answer_template="Revenue for the last {n} days on record:\n{rows}",
    ),
]
