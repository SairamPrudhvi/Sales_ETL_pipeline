import pandas as pd
import numpy as np
import random
from faker import Faker
import os

fake = Faker()
np.random.seed(42)

current_path = os.getcwd()

output_dir = os.path.join(current_path, "raw_data")

os.makedirs(output_dir, exist_ok=True)

# -----------------------------
# Customers
# -----------------------------
customers = []
for i in range(1, 1101):
    customers.append({
        "customer_id": i if random.random() > 0.02 else random.randint(1, 50),  # duplicates
        "customer_name": fake.name().upper() if random.random() > 0.1 else "  " + fake.name(),
        "email": fake.email() if random.random() > 0.1 else None,
        "city": fake.city() if random.random() > 0.1 else None,
        "state": fake.state_abbr() if random.random() > 0.1 else "",
        "signup_date": fake.date_between(start_date="-5y", end_date="today")
        if random.random() > 0.05 else "invalid_date"
    })

customers_df = pd.DataFrame(customers)
file_path = os.path.join(output_dir, "customers.csv")
customers_df.to_csv(file_path, index=False)

# -----------------------------
# Products
# -----------------------------
categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]

products = []
for i in range(1, 1101):
    products.append({
        "product_id": i if random.random() > 0.02 else random.randint(1, 100),
        "product_name": fake.word().title(),
        "category": random.choice(categories),
        "brand": fake.company() if random.random() > 0.1 else None,
        "unit_price": round(random.uniform(5, 500), 2)
        if random.random() > 0.1 else random.choice([None, -10, 0])
    })

products_df = pd.DataFrame(products)
file_path = os.path.join(output_dir, "products.csv")
products_df.to_csv(file_path, index=False)

# -----------------------------
# Sales Transactions
# -----------------------------
sales = []
for i in range(1, 5501):
    sales.append({
        "transaction_id": i if random.random() > 0.03 else random.randint(1, 200),
        "customer_id": random.randint(1, 1200),  # orphan customers included
        "product_id": random.randint(1, 1200),   # orphan products included
        "transaction_date": fake.date_between(start_date="-2y", end_date="today")
        if random.random() > 0.05 else "2023-99-99",
        "quantity": random.randint(1, 10)
        if random.random() > 0.1 else random.choice([None, -3, 0]),
        "unit_price": round(random.uniform(5, 500), 2)
        if random.random() > 0.1 else random.choice([None, -20, 0]),
        "discount": round(random.uniform(0, 50), 2)
        if random.random() > 0.8 else 0
    })

sales_df = pd.DataFrame(sales)
file_path = os.path.join(output_dir, "sales_transactions.csv")
sales_df.to_csv(file_path, index=False)

print("CSV files generated successfully!")