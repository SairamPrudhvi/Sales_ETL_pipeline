from etl.logging_config import setup_logging

setup_logging()   

from etl.pipeline import SalesETLPipeline
pipeline = SalesETLPipeline()
pipeline.extract()
pipeline.validate()


# CLEAN DATA CHECKS

print("=== CLEAN DATA SHAPES ===")
print("Customers:", pipeline.customers.shape)
print("Products:", pipeline.products.shape)
print("Sales:", pipeline.sales.shape)

print("\n=== SALES SAMPLE AFTER VALIDATION ===")
print(pipeline.sales.info())



# # REJECTED DATA CHECKS

# print("\n=== REJECTED BATCHES ===")
# for i, df in enumerate(pipeline.rejected_records, start=1):
#     print(f"\nRejected batch {i}")
#     print("Shape:", df.shape)
#     print(df.head())
