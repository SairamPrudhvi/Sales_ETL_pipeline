# etl/transform/product_transform.py

import pandas as pd

def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ---------- BRAND ----------
    if df["brand"].notna().any():
        brand_mode = df["brand"].mode(dropna=True)[0]
    else:
        brand_mode = "unknown"

    df["brand"] = (
        df["brand"]
        .fillna(brand_mode)
        .str.strip()
        .str.lower()
    )

    # ---------- CATEGORY ----------
    if df["category"].notna().any():
        category_mode = df["category"].mode(dropna=True)[0]
    else:
        category_mode = "unknown"

    df["category"] = (
        df["category"]
        .fillna(category_mode)
        .str.strip()
        .str.lower()
    )

    # ---------- PRODUCT NAME ----------
    df["product_name"] = (
        df["product_name"]
        .str.strip()
        .str.lower()
    )

    return df
