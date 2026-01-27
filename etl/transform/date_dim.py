import pandas as pd

def build_dim_date(dates: pd.Series) -> pd.DataFrame:
    dim_date = pd.DataFrame({"date": dates.drop_duplicates()})
    dim_date["date_id"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["weekday"] = dim_date["date"].dt.day_name()

    return dim_date
