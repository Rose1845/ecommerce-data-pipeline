from log.logging import setup_logging
logger = setup_logging()


def extract_sales_data(spark, file_path):
    """Extract sales data from a CSV file"""
    logger.info(f"Extracting data from {file_path}")
    return spark.read.csv(file_path, header=True, inferSchema=True)


def extract_all_data(spark):
    """Combine data from multiple sources via joins"""

    orders = extract_sales_data(spark, "data/orders.csv")
    orders.write.mode("overwrite").parquet(
        "data/orders.parquet").saveasTable("orders")
    print(orders.schema)
    order_items = extract_sales_data(spark, "data/order_items.csv")
    print(order_items.schema)

    customers = extract_sales_data(spark, "data/customers.csv")
    customers.write.mode("overwrite").parquet(
        "data/customers.parquet").saveasTable("customers")
    print(customers.schema)

    orders_with_items = orders.join(order_items, on="order_id", how="left")

    all_orders = orders_with_items.join(
        customers, on="customer_id", how="left")

    print("Data extraction complete. Sample data:")
    all_orders.show(5)

    count = all_orders.count()
    print(f"Combined dataset has {count} rows")
    logger.info(f"Combined dataset has {count} rows")

    return all_orders
