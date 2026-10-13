# Apache Spark Sales Analysis

A beginner-friendly PySpark batch job for validating e-commerce transactions, calculating revenue, and generating summaries by product and city. Spark runs inside Docker; no local Java or Python installation is required.

## Requirements

- Docker Desktop (or Docker Engine) with the Docker Compose plugin.
- The repository cloned to your machine.

## Run the analysis

From the repository root:

    docker compose up --build

The container reads data/sales.csv, runs Spark in local mode using all available container CPU cores, and writes results to the output directory. The container exits when the batch job is complete. To run it again, execute the same command.

Alternatively, run only the one-shot service:

    docker compose run --build --rm sales-analysis

To validate the output files after a successful run:

    python scripts/verify_outputs.py

The verification script uses only Python's standard library. It checks the expected totals, product and city leaders, and the three rejected orders.

## Input

The input CSV contains these required columns:

| Column | Meaning |
| --- | --- |
| order_id | Transaction identifier |
| order_date | Date in YYYY-MM-DD format |
| customer_id | Customer identifier |
| city | Customer city |
| product | Product name |
| quantity | Units sold; must be a positive whole number |
| unit_price | Price per unit; must be greater than zero |

To analyze a different file, replace data/sales.csv while preserving the header, or change INPUT_PATH in docker-compose.yml. OUTPUT_DIR can be changed the same way.

## Validation rules

A row is valid only when all seven required fields are present and non-blank, order_date is a valid date in the specified format, quantity is numeric, positive, and a whole number, and unit_price is numeric and greater than zero. Invalid rows are retained in a separate output with a validation_errors explanation. They are excluded from every sales metric.

The job deliberately does not reject repeated order IDs because uniqueness is not a rule in the exercise.

## Output files

Spark writes CSV datasets as directories containing one or more part files. Open the folder and use its CSV part file(s), or load the whole directory with a compatible CSV reader.

- output/valid_transactions/ — accepted transactions, including revenue for each order.
- output/invalid_transactions/ — rejected source records and validation_errors.
- output/product_summary/ — units sold and total revenue by product.
- output/city_summary/ — total revenue by city.
- output/kpis/ — one-row CSV with overall metrics and the leading product and city.
- output/analysis_summary.json — machine-readable summary.
- output/analysis_summary.txt — readable summary of the key indicators.
- output/validation_notes.txt — business rules and information about rejected rows.

Output files are generated at runtime and are excluded from version control. Run the job first to populate them.

## Expected results for the included sample

| Indicator | Expected result |
| --- | ---: |
| Transactions processed | 15 |
| Valid transactions | 12 |
| Invalid transactions | 3 |
| Units sold in valid transactions | 24 |
| Total valid revenue | COP 3,975,000.00 |
| Highest-revenue product | Monitor — COP 2,340,000.00 |
| Highest-revenue city | Bogota — COP 1,900,000.00 |

The invalid order IDs are 1008 (quantity is zero), 1009 (quantity is negative), and 1012 (unit price is negative).

## Project structure

    .
    ├── data/sales.csv
    ├── src/sales_analysis.py
    ├── scripts/verify_outputs.py
    ├── Dockerfile
    ├── docker-compose.yml
    └── output/

## Implementation notes

- Apache Spark 4.2.0 is provided by the official Apache Spark Docker image, with Python and Java included.
- The code uses Spark DataFrames for parsing, validation, aggregation, and CSV output.
- Output directories are overwritten on each run so old rows cannot leak into new results.
- The Compose service uses the container's root user to make the mounted output directory writable across common desktop setups. This is for a local learning exercise, not a production security configuration.
