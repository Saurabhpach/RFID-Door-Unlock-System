"""etl.py
Simple, reusable ETL (Extract-Transform-Load) pipeline implementation.

This script demonstrates a generic pattern you can adapt for real-world jobs:
1. Extract  – read data from a source (CSV, database, API, etc.).
2. Transform – clean or enrich the data.
3. Load     – persist the results to a target store (SQLite used here for simplicity).

Usage (from shell):
    python etl.py --source data/raw/customers.csv \
                  --target-db warehouse.db \
                  --table customers_clean

Dependencies:
    pip install pandas sqlalchemy
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any, Callable

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_from_csv(csv_path: str | Path, **read_csv_kwargs: Any) -> pd.DataFrame:
    """Read a CSV file into a DataFrame.

    Parameters
    ----------
    csv_path : str | Path
        Location of the CSV file.
    **read_csv_kwargs : Any
        Additional keyword arguments passed to `pandas.read_csv`.

    Returns
    -------
    pd.DataFrame
        Raw data extracted from the CSV.
    """
    logger.info("Extracting data from %s", csv_path)
    df = pd.read_csv(csv_path, **read_csv_kwargs)
    logger.info("Extracted %d rows and %d columns", len(df), len(df.columns))
    return df

# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def default_transform(df: pd.DataFrame) -> pd.DataFrame:
    """An example transform that cleans column names and drops rows with NA."""
    logger.info("Starting transformation step")

    # Standardize column names: lowercase & snake_case-like
    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_", regex=False)
    )

    before = len(df)
    df = df.dropna(how="all")  # remove completely empty rows
    after = len(df)
    logger.info("Dropped %d empty rows", before - after)

    logger.info("Transformation finished; resulting shape: %s", df.shape)
    return df

# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load_to_sqlite(df: pd.DataFrame, engine: Engine, table_name: str) -> None:
    """Load DataFrame into a SQLite table, replacing existing data."""
    logger.info("Loading dataframe into table '%s'", table_name)
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    logger.info("Successfully loaded %d records into '%s'", len(df), table_name)

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_etl(
    source_csv: str | Path,
    target_db: str | Path,
    table_name: str,
    *,
    extract_func: Callable[..., pd.DataFrame] = extract_from_csv,
    transform_func: Callable[[pd.DataFrame], pd.DataFrame] = default_transform,
) -> None:
    """Run the ETL pipeline using provided callables."""
    df_raw = extract_func(source_csv)
    df_clean = transform_func(df_raw)

    engine = create_engine(f"sqlite:///{Path(target_db).expanduser().resolve()}")
    load_to_sqlite(df_clean, engine, table_name)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:  # noqa: D401
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run a simple CSV→SQLite ETL pipeline")
    parser.add_argument("--source", required=True, help="Path to input CSV file")
    parser.add_argument(
        "--target-db", required=True, help="Path to the output SQLite database file"
    )
    parser.add_argument(
        "--table", required=True, help="Destination table name inside SQLite database"
    )
    return parser.parse_args()


def main() -> None:  # noqa: D401
    """Script entry point."""
    args = _parse_args()
    run_etl(args.source, args.target_db, args.table)


if __name__ == "__main__":
    main()