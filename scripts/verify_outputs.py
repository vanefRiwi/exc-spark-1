"""Check the Spark job's generated outputs against the supplied sample."""

from __future__ import annotations

import csv
import json
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output"


def read_csv_dataset(dataset_name: str) -> list[dict[str, str]]:
    dataset_dir = OUTPUT_DIR / dataset_name
    part_files = sorted(dataset_dir.glob("part-*.csv"))
    if not part_files:
        raise AssertionError(f"No CSV part files found in {dataset_dir}")

    rows: list[dict[str, str]] = []
    for part_file in part_files:
        with part_file.open("r", encoding="utf-8", newline="") as csv_file:
            rows.extend(csv.DictReader(csv_file))
    return rows


def main() -> None:
    summary_path = OUTPUT_DIR / "analysis_summary.json"
    if not summary_path.is_file():
        raise FileNotFoundError(
            "No analysis summary found. Run 'docker compose up --build' first."
        )

    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert summary["total_records"] == 15, summary
    assert summary["valid_records"] == 12, summary
    assert summary["invalid_records"] == 3, summary
    assert summary["total_units_sold"] == 24, summary
    assert Decimal(summary["total_valid_revenue_cop"]) == Decimal("3975000.00")
    assert summary["top_product"]["name"] == "Monitor", summary
    assert Decimal(summary["top_product"]["revenue_cop"]) == Decimal("2340000.00")
    assert summary["top_city"]["name"] == "Bogota", summary
    assert Decimal(summary["top_city"]["revenue_cop"]) == Decimal("1900000.00")

    valid_rows = read_csv_dataset("valid_transactions")
    invalid_rows = read_csv_dataset("invalid_transactions")
    products = read_csv_dataset("product_summary")
    cities = read_csv_dataset("city_summary")

    assert len(valid_rows) == 12, f"Expected 12 valid rows, found {len(valid_rows)}"
    assert len(invalid_rows) == 3, f"Expected 3 invalid rows, found {len(invalid_rows)}"
    assert {row["order_id"] for row in invalid_rows} == {"1008", "1009", "1012"}

    product_lookup = {row["product"]: row for row in products}
    assert int(product_lookup["Monitor"]["units_sold"]) == 3
    assert Decimal(product_lookup["Monitor"]["total_revenue"]) == Decimal("2340000.00")

    city_lookup = {row["city"]: row for row in cities}
    assert Decimal(city_lookup["Bogota"]["total_revenue"]) == Decimal("1900000.00")

    assert all(row["validation_errors"].strip() for row in invalid_rows)
    print("PASS: Spark output matches the expected sample results.")


if __name__ == "__main__":
    main()
