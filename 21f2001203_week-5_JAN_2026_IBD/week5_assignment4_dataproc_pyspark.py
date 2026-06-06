import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def normalize_city(city_col):
    """Normalize city names and map common variants to a consistent format."""
    city_clean = F.initcap(F.trim(city_col))
    city_lower = F.lower(F.trim(city_col))

    return (
        F.when(city_lower.isin("kochi", "cochin"), F.lit("Kochi"))
        .when(city_lower.isin("trivandrum", "thiruvananthapuram"), F.lit("Trivandrum"))
        .when(city_lower.isin("bangalore", "bengaluru"), F.lit("Bengaluru"))
        .when(city_lower.isin("cmbt", "chennai"), F.lit("Chennai"))
        .otherwise(city_clean)
    )


def is_blank(col_name):
    return F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")


def mark_duplicates(df, all_cols, prefix):
    """Keep the first full-row instance and mark remaining identical rows as duplicates."""
    window_spec = Window.partitionBy(*[F.col(c) for c in all_cols]).orderBy(F.monotonically_increasing_id())
    return df.withColumn(f"{prefix}_dup_rank", F.row_number().over(window_spec))


def write_csv(df, path):
    df.write.mode("overwrite").option("header", True).csv(path)


def main():
    parser = argparse.ArgumentParser(description="Week 5 Graded Assignment 4 - Dataproc PySpark")
    parser.add_argument("--customer_input", required=True, help="GCS path for customer CSV")
    parser.add_argument("--transaction_input", required=True, help="GCS path for transaction CSV")
    parser.add_argument("--output_base", required=True, help="GCS base output folder")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Week5_GA4_Dataproc_PySpark")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # 1) Load and Inspect
    customer_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.customer_input)
    )

    transaction_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.transaction_input)
    )

    print("===== Customer Schema =====")
    customer_raw.printSchema()
    print("===== Customer Sample =====")
    customer_raw.show(10, truncate=False)

    print("===== Transaction Schema =====")
    transaction_raw.printSchema()
    print("===== Transaction Sample =====")
    transaction_raw.show(10, truncate=False)

    # 2 + 3) Column Transformations and Data Cleaning (separate cleaning)
    # Customer cleaning
    customer_stage = (
        customer_raw
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("customer_name", F.trim(F.col("customer_name")))
        .withColumn("city", normalize_city(F.col("city")))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
    )

    customer_with_dups = mark_duplicates(
        customer_stage,
        ["customer_id", "customer_name", "city", "status"],
        "cust"
    )

    customer_invalid_cond = (
        is_blank("customer_id") |
        is_blank("customer_name") |
        is_blank("city") |
        is_blank("status") |
        (~F.col("customer_id").rlike(r"^c\d+$")) |
        (~F.col("status").isin("active", "inactive")) |
        (F.col("cust_dup_rank") > 1)
    )

    customer_invalid = (
        customer_with_dups
        .where(customer_invalid_cond)
        .withColumn(
            "invalid_reason",
            F.when(is_blank("customer_id"), F.lit("missing_customer_id"))
            .when(is_blank("customer_name"), F.lit("missing_customer_name"))
            .when(is_blank("city"), F.lit("missing_city"))
            .when(is_blank("status"), F.lit("missing_status"))
            .when(~F.col("customer_id").rlike(r"^c\d+$"), F.lit("invalid_customer_id_format"))
            .when(~F.col("status").isin("active", "inactive"), F.lit("invalid_status"))
            .when(F.col("cust_dup_rank") > 1, F.lit("duplicate_record"))
            .otherwise(F.lit("other_invalid"))
        )
    )

    customer_clean = (
        customer_with_dups
        .where(~customer_invalid_cond)
        .drop("cust_dup_rank")
    )

    print("===== Invalid Customer Rows (sample) =====")
    customer_invalid.show(10, truncate=False)
    print("===== Cleaned Customer Rows (sample) =====")
    customer_clean.show(10, truncate=False)

    # Transaction cleaning
    transaction_stage = (
        transaction_raw
        .withColumn("transaction_id", F.trim(F.col("transaction_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("transaction_date_str", F.trim(F.col("transaction_date").cast("string")))
        .withColumn("transaction_amount", F.col("transaction_amount").cast("double"))
        .withColumn("payment_mode", F.lower(F.trim(F.col("payment_mode"))))
    )

    transaction_stage = (
        transaction_stage
        .withColumn(
            "transaction_date",
            F.to_date(F.col("transaction_date_str"), "yyyy-MM-dd")
        )
        .withColumn(
            "amount_category",
            F.when(F.col("transaction_amount") < 3000, F.lit("Low"))
            .when(F.col("transaction_amount") < 7000, F.lit("Medium"))
            .otherwise(F.lit("High"))
        )
        .withColumn("transaction_month", F.month(F.col("transaction_date")))
    )

    transaction_with_dups = mark_duplicates(
        transaction_stage,
        [
            "transaction_id", "customer_id", "transaction_date_str",
            "transaction_amount", "payment_mode"
        ],
        "txn"
    )

    base_txn_invalid_cond = (
        is_blank("transaction_id") |
        is_blank("customer_id") |
        is_blank("payment_mode") |
        F.col("transaction_amount").isNull() |
        (F.col("transaction_amount") <= 0) |
        is_blank("transaction_date_str") |
        (~F.col("transaction_date_str").rlike(r"^\d{4}-\d{2}-\d{2}$")) |
        F.col("transaction_date").isNull() |
        (F.col("txn_dup_rank") > 1)
    )

    transaction_invalid_pre_ref = (
        transaction_with_dups
        .where(base_txn_invalid_cond)
        .withColumn(
            "invalid_reason",
            F.when(is_blank("transaction_id"), F.lit("missing_transaction_id"))
            .when(is_blank("customer_id"), F.lit("missing_customer_id"))
            .when(is_blank("payment_mode"), F.lit("missing_payment_mode"))
            .when(F.col("transaction_amount").isNull(), F.lit("invalid_transaction_amount_type"))
            .when(F.col("transaction_amount") <= 0, F.lit("non_positive_transaction_amount"))
            .when(is_blank("transaction_date_str"), F.lit("missing_transaction_date"))
            .when(~F.col("transaction_date_str").rlike(r"^\d{4}-\d{2}-\d{2}$"), F.lit("invalid_transaction_date_format"))
            .when(F.col("transaction_date").isNull(), F.lit("malformed_transaction_date"))
            .when(F.col("txn_dup_rank") > 1, F.lit("duplicate_record"))
            .otherwise(F.lit("other_invalid"))
        )
    )

    transaction_candidate = transaction_with_dups.where(~base_txn_invalid_cond)

    # Referential integrity: all transaction customer_id should exist in cleaned customer data
    valid_customer_ids = customer_clean.select("customer_id").distinct()

    transaction_invalid_ref = (
        transaction_candidate
        .join(valid_customer_ids, on="customer_id", how="left_anti")
        .withColumn("invalid_reason", F.lit("customer_id_not_found_in_customer_dataset"))
    )

    transaction_clean = (
        transaction_candidate
        .join(valid_customer_ids, on="customer_id", how="left_semi")
        .drop("txn_dup_rank")
    )

    transaction_invalid = transaction_invalid_pre_ref.unionByName(
        transaction_invalid_ref,
        allowMissingColumns=True
    )

    print("===== Invalid Transaction Rows (sample) =====")
    transaction_invalid.show(10, truncate=False)
    print("===== Cleaned Transaction Rows (sample) =====")
    transaction_clean.show(10, truncate=False)

    # Save cleaned_data and invalid_rows
    output_base = args.output_base.rstrip("/")

    write_csv(customer_clean, f"{output_base}/cleaned_data/customers")
    write_csv(transaction_clean, f"{output_base}/cleaned_data/transactions")
    write_csv(customer_invalid, f"{output_base}/invalid_rows/customers")
    write_csv(transaction_invalid, f"{output_base}/invalid_rows/transactions")

    # 4) Join
    joined_data = (
        transaction_clean.alias("t")
        .join(customer_clean.alias("c"), on="customer_id", how="inner")
        .select(
            F.col("t.transaction_id"),
            F.col("t.customer_id"),
            F.col("c.customer_name"),
            F.col("c.city"),
            F.col("c.status"),
            F.col("t.transaction_date"),
            F.col("t.transaction_month"),
            F.col("t.transaction_amount"),
            F.col("t.amount_category"),
            F.col("t.payment_mode")
        )
    )

    print("===== Joined Data (sample) =====")
    joined_data.show(10, truncate=False)

    write_csv(joined_data, f"{output_base}/joined_data")

    # 5) Aggregations
    total_avg_per_customer = (
        joined_data
        .groupBy("customer_id", "customer_name")
        .agg(
            F.round(F.sum("transaction_amount"), 2).alias("total_transaction_amount"),
            F.round(F.avg("transaction_amount"), 2).alias("avg_transaction_amount")
        )
        .orderBy(F.desc("total_transaction_amount"))
    )

    total_per_city = (
        joined_data
        .groupBy("city")
        .agg(F.round(F.sum("transaction_amount"), 2).alias("total_transaction_amount"))
        .orderBy(F.desc("total_transaction_amount"))
    )

    top3_customers = total_avg_per_customer.limit(3)

    print("===== Aggregation: Total/Avg per Customer (sample) =====")
    total_avg_per_customer.show(10, truncate=False)
    print("===== Aggregation: Total per City (sample) =====")
    total_per_city.show(10, truncate=False)
    print("===== Aggregation: Top 3 Customers =====")
    top3_customers.show(3, truncate=False)

    write_csv(total_avg_per_customer, f"{output_base}/aggregates/total_avg_per_customer")
    write_csv(total_per_city, f"{output_base}/aggregates/total_per_city")
    write_csv(top3_customers, f"{output_base}/aggregates/top3_customers")

    spark.stop()


if __name__ == "__main__":
    main()
