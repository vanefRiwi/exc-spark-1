"""Validate and summarize sales transactions using Apache Spark."""

from __future__ import annotations

import json
import os
import shutil
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


REQUIRED_FIELDS = (
    "order_id",
    "order_date",
    "customer_id",
    "city",
    "product",
    "quantity",
    "unit_price",
)


def add_validation_errors(raw_sales: DataFrame) -> DataFrame:
    """Add parsed values and a human-readable validation error string."""
    quantity_text = F.trim(F.coalesce(F.col("quantity"), F.lit("")))
    price_text = F.trim(F.coalesce(F.col("unit_price"), F.lit("")))
    date_text = F.trim(F.coalesce(F.col("order_date"), F.lit("")))

    sales = (
        raw_sales
        .withColumn(
            "_order_date_parsed",
            F.try_to_timestamp(
                F.trim(F.col("order_date")), F.lit("yyyy-MM-dd")
            ).cast("date"),
        )
        .withColumn(
            "_quantity_number",
            F.expr("try_cast(trim(quantity) AS DOUBLE)"),
        )
        .withColumn(
            "_unit_price_number",
            F.expr("try_cast(trim(unit_price) AS DECIMAL(18, 2))"),
        )
    )

    error_expressions = []
    for field in REQUIRED_FIELDS:
        field_text = F.trim(F.coalesce(F.col(field), F.lit("")))
        error_expressions.append(
            F.when(field_text == "", F.lit(f"{field} is required"))
        )

    error_expressions.extend(
        [
            F.when(
                (date_text != "") & F.col("_order_date_parsed").isNull(),
                F.lit("order_date must be a valid date in YYYY-MM-DD format"),
            ),
            F.when(
                (quantity_text != "") & F.col("_quantity_number").isNull(),
                F.lit("quantity must be numeric"),
            ),
            F.when(
                F.col("_quantity_number").isNotNull()
                & (
                    F.isnan(F.col("_quantity_number"))
                    | (F.col("_quantity_number") <= 0)
                ),
                F.lit("quantity must be greater than zero"),
            ),
            F.when(
                F.col("_quantity_number").isNotNull()
                & (
                    (F.col("_quantity_number") != F.floor(F.col("_quantity_number")))
                    | (F.col("_quantity_number") > 2147483647)
                ),
                F.lit("quantity must be a whole number within the supported range"),
            ),
            F.when(
                (price_text != "") & F.col("_unit_price_number").isNull(),
                F.lit("unit_price must be numeric"),
            ),
            F.when(
                F.col("_unit_price_number").isNotNull()
                & (F.col("_unit_price_number") <= 0),
                F.lit("unit_price must be greater than zero"),
            ),
        ]
    )

    return sales.withColumn(
        "_validation_errors",
        F.concat_ws("; ", *error_expressions),
    )


def write_csv(dataframe: DataFrame, destination: Path, single_file: bool = False) -> None:
    """Write a CSV dataset, replacing results from any previous run."""
    writer_df = dataframe.coalesce(1) if single_file else dataframe
    (
        writer_df.write
        .mode("overwrite")
        .option("header", "true")
        .option("emptyValue", "")
        .csv(str(destination))
    )


def money_as_text(value: Decimal | None) -> str:
    """Format money as a decimal string without floating-point rounding."""
    amount = value if value is not None else Decimal("0.00")
    return format(amount, ".2f")


def main() -> None:
    input_path = Path(os.getenv("INPUT_PATH", "/opt/spark-app/data/sales.csv"))
    output_dir = Path(os.getenv("OUTPUT_DIR", "/opt/spark-app/output"))
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("JuniorSalesAnalysis")
        .getOrCreate()
    )

    try:
        if not input_path.is_file():
            raise FileNotFoundError(
                f"Input CSV was not found: {input_path}. "
                "Check INPUT_PATH and the Docker Compose data volume."
            )

        raw_sales = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .csv(str(input_path))
        )

        missing_fields = sorted(set(REQUIRED_FIELDS) - set(raw_sales.columns))
        if missing_fields:
            raise ValueError(
                "Input CSV is missing required columns: "
                + ", ".join(missing_fields)
            )

        prepared_sales = add_validation_errors(raw_sales).cache()
        count_row = prepared_sales.agg(
            F.count(F.lit(1)).alias("total_records"),
            F.sum(
                F.when(F.col("_validation_errors") == "", 1).otherwise(0)
            ).alias("valid_records"),
            F.sum(
                F.when(F.col("_validation_errors") != "", 1).otherwise(0)
            ).alias("invalid_records"),
        ).first()

        total_records = int(count_row["total_records"] or 0)
        valid_count = int(count_row["valid_records"] or 0)
        invalid_count = int(count_row["invalid_records"] or 0)

        valid_sales = (
            prepared_sales
            .filter(F.col("_validation_errors") == "")
            .select(
                F.trim(F.col("order_id")).alias("order_id"),
                F.col("_order_date_parsed").cast("string").alias("order_date"),
                F.trim(F.col("customer_id")).alias("customer_id"),
                F.trim(F.col("city")).alias("city"),
                F.trim(F.col("product")).alias("product"),
                F.col("_quantity_number").cast("long").alias("quantity"),
                F.col("_unit_price_number").alias("unit_price"),
            )
            .withColumn(
                "revenue",
                F.col("quantity").cast("decimal(18, 0)") * F.col("unit_price"),
            )
            .cache()
        )

        invalid_sales = (
            prepared_sales
            .filter(F.col("_validation_errors") != "")
            .select(
                *[F.col(field) for field in REQUIRED_FIELDS],
                F.col("_validation_errors").alias("validation_errors"),
            )
            .cache()
        )

        product_summary = (
            valid_sales
            .groupBy("product")
            .agg(
                F.sum("quantity").cast("long").alias("units_sold"),
                F.sum("revenue").alias("total_revenue"),
            )
            .orderBy(F.desc("total_revenue"), F.asc("product"))
        )

        city_summary = (
            valid_sales
            .groupBy("city")
            .agg(F.sum("revenue").alias("total_revenue"))
            .orderBy(F.desc("total_revenue"), F.asc("city"))
        )

        totals = valid_sales.agg(
            F.sum("quantity").alias("total_units_sold"),
            F.sum("revenue").alias("total_valid_revenue"),
        ).first()

        total_units = int(totals["total_units_sold"] or 0)
        total_revenue = totals["total_valid_revenue"] or Decimal("0.00")
        top_product = product_summary.first()
        top_city = city_summary.first()

        top_product_name = top_product["product"] if top_product else ""
        top_product_revenue = top_product["total_revenue"] if top_product else Decimal("0.00")
        top_city_name = top_city["city"] if top_city else ""
        top_city_revenue = top_city["total_revenue"] if top_city else Decimal("0.00")

        output_names = (
            "valid_transactions",
            "invalid_transactions",
            "product_summary",
            "city_summary",
            "kpis",
        )
        for name in output_names:
            existing_path = output_dir / name
            if existing_path.is_dir():
                shutil.rmtree(existing_path)

        for report_name in ("analysis_summary.json", "analysis_summary.txt", "validation_notes.txt"):
            report_path = output_dir / report_name
            if report_path.exists():
                report_path.unlink()

        write_csv(
            valid_sales.select(
                "order_id", "order_date", "customer_id", "city",
                "product", "quantity", "unit_price", "revenue",
            ),
            output_dir / "valid_transactions",
        )
        write_csv(
            invalid_sales,
            output_dir / "invalid_transactions",
        )
        write_csv(product_summary, output_dir / "product_summary", single_file=True)
        write_csv(city_summary, output_dir / "city_summary", single_file=True)

        kpis = {
            "total_records": total_records,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "total_units_sold": total_units,
            "total_valid_revenue_cop": money_as_text(total_revenue),
            "top_product": top_product_name,
            "top_product_revenue_cop": money_as_text(top_product_revenue),
            "top_city": top_city_name,
            "top_city_revenue_cop": money_as_text(top_city_revenue),
        }
        kpis_schema = (
            "total_records BIGINT, valid_records BIGINT, invalid_records BIGINT, "
            "total_units_sold BIGINT, total_valid_revenue_cop STRING, "
            "top_product STRING, top_product_revenue_cop STRING, "
            "top_city STRING, top_city_revenue_cop STRING"
        )
        spark.createDataFrame([kpis], schema=kpis_schema).write.mode("overwrite").option(
            "header", "true"
        ).csv(str(output_dir / "kpis"))

        (output_dir / "analysis_summary.json").write_text(
            json.dumps(
                {
                    **kpis,
                    "currency": "COP",
                    "top_product": (
                        {
                            "name": top_product_name,
                            "revenue_cop": money_as_text(top_product_revenue),
                        }
                        if top_product
                        else None
                    ),
                    "top_city": (
                        {
                            "name": top_city_name,
                            "revenue_cop": money_as_text(top_city_revenue),
                        }
                        if top_city
                        else None
                    ),
                },
                indent=2,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )

        summary_lines = [
            "SALES ANALYSIS SUMMARY",
            "======================",
            f"Records processed: {total_records}",
            f"Valid transactions: {valid_count}",
            f"Invalid transactions: {invalid_count}",
            f"Units sold in valid transactions: {total_units}",
            f"Total valid revenue: COP {total_revenue:,.2f}",
            f"Highest-revenue product: {top_product_name or 'N/A'} "
            f"(COP {top_product_revenue:,.2f})",
            f"Highest-revenue city: {top_city_name or 'N/A'} "
            f"(COP {top_city_revenue:,.2f})",
        ]
        (output_dir / "analysis_summary.txt").write_text(
            "\n".join(summary_lines) + "\n",
            encoding="utf-8",
        )

        validation_notes = (
            "SALES VALIDATION NOTES\n"
            "======================\n"
            "A valid transaction must have all seven required fields populated. "
            "order_date must be a valid date in YYYY-MM-DD format; quantity must "
            "be numeric, positive, and a whole number; unit_price must be numeric "
            "and greater than zero.\n\n"
            "Invalid transactions are excluded from revenue and unit calculations. "
            "See invalid_transactions/ for the original rows and their "
            "validation_errors. For the supplied sample, orders 1008, 1009, and "
            "1012 are rejected because quantity is zero, quantity is negative, "
            "and unit_price is negative, respectively.\n"
        )
        (output_dir / "validation_notes.txt").write_text(
            validation_notes,
            encoding="utf-8",
        )

        print("\n".join(summary_lines))
        print(f"\nOutput files written to: {output_dir.resolve()}")

        valid_sales.unpersist()
        invalid_sales.unpersist()
        prepared_sales.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
