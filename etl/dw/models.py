from sqlalchemy import (
    Column, Integer, String, Float, Date, ForeignKey
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DimCustomer(Base):
    __tablename__ = "dim_customer"
    __table_args__ = {"schema": "sales_dw"}

    customer_id = Column(Integer, primary_key=True)
    customer_name = Column(String)
    email = Column(String)
    city = Column(String)
    state = Column(String)
    signup_date = Column(Date)

class DimProduct(Base):
    __tablename__ = "dim_product"
    __table_args__ = {"schema": "sales_dw"}

    product_id = Column(Integer, primary_key=True)
    product_name = Column(String)
    category = Column(String)
    brand = Column(String)
    unit_price = Column(Float)

class DimDate(Base):
    __tablename__ = "dim_date"
    __table_args__ = {"schema": "sales_dw"}

    date_id = Column(Integer, primary_key=True)
    date = Column(Date)
    day = Column(Integer)
    month = Column(Integer)
    quarter = Column(Integer)
    year = Column(Integer)
    weekday = Column(String)

class FactSales(Base):
    __tablename__ = "fact_sales"
    __table_args__ = {"schema": "sales_dw"}

    sales_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, ForeignKey("sales_dw.dim_customer.customer_id"))
    product_id = Column(Integer, ForeignKey("sales_dw.dim_product.product_id"))
    date_id = Column(Integer, ForeignKey("sales_dw.dim_date.date_id"))
    quantity = Column(Integer)
    unit_price = Column(Float)
    total_sale_amount = Column(Float)
    net_sale_amount = Column(Float)
