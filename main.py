import logging
import os
import sys
import time
from dataclasses import dataclass, field, fields
from typing import Dict, List, get_type_hints

import duckdb
import pandas

logging.basicConfig(
    level=logging.DEBUG,  # Set logging level to DEBUG to capture detailed logs
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)

# Map Python types to DuckDB types
TYPE_MAPPING = {
    int: "INT",
    str: "STRING",
    float: "DOUBLE",
    bool: "BOOLEAN",
}

BATCH_SIZE = 1000


@dataclass
class TaxiTrip:
    """
    Dataclass representing a taxi trip.

    This class is used to normalize raw data from various taxi trip datasets into a standard format.
    Field names follow a normalized convention (lowercase with underscores), while aliases are used
    to map any variations in field names from raw files (e.g., VendorID vs vendor_id). Default values
    are provided to handle missing data in raw files.
    """

    vendor_id: int = field(default=0, metadata={"aliases": ["VendorID"]})
    tpep_pickup_datetime: str = field(default=None, metadata={})
    tpep_dropoff_datetime: str = field(default=None, metadata={})
    passenger_count: int = field(default=0, metadata={})
    trip_distance: float = field(default=0.0, metadata={})
    ratecode_id: int = field(default=0, metadata={"aliases": ["RatecodeID"]})
    store_and_fwd_flag: str = field(default=None, metadata={})
    pu_location_id: int = field(default=0, metadata={"aliases": ["PULocationID"]})
    do_location_id: int = field(default=0, metadata={"aliases": ["DOLocationID"]})
    payment_type: int = field(default=0, metadata={})
    fare_amount: float = field(default=0.0, metadata={})
    extra: float = field(default=0.0, metadata={})
    mta_tax: float = field(default=0.0, metadata={})
    tip_amount: float = field(default=0.0, metadata={})
    tolls_amount: float = field(default=0.0, metadata={})
    improvement_surcharge: float = field(default=0.0, metadata={})
    total_amount: float = field(default=0.0, metadata={})
    congestion_surcharge: float = field(default=0.0, metadata={})
    airport_fee: float = field(default=0.0, metadata={"aliases": ["Airport_fee"]})


def map_python_type_to_duckdb(field: field) -> str:
    """Maps Python type to DuckDB type."""
    field_type = get_type_hints(TaxiTrip).get(field.name)
    return TYPE_MAPPING.get(
        field_type, "STRING"
    )  # Default to STRING if type is not in the mapping


def create_table_from_dataclass(con, table_name: str, dataclass):
    """Generate DuckDB CREATE TABLE schema from the dataclass and execute it."""
    fields_definitions = []

    for field_obj in fields(dataclass):
        duckdb_type = map_python_type_to_duckdb(field_obj)
        fields_definitions.append(f"{field_obj.name} {duckdb_type}")

    fields_definitions.append("file_name STRING")  # Add a column for the file name

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join(fields_definitions)}
    );
    """

    # Execute the dynamically generated SQL
    con.execute(create_table_sql)


def precompute_field_mappings() -> dict:
    """Pre-compute a mapping from raw file field names (including all aliases) to normalized dataclass field names."""
    field_mapping = {}

    # Iterate through the fields in the TaxiTrip dataclass
    for field_obj in fields(TaxiTrip):
        # Map the field name to itself
        normalized_field_name = field_obj.name
        field_mapping[normalized_field_name] = normalized_field_name

        # Add all aliases to the mapping
        aliases = field_obj.metadata.get("aliases", [])
        for alias in aliases:
            field_mapping[alias] = normalized_field_name

    return field_mapping


def load_parquet_files(directory: str) -> List[pandas.DataFrame]:
    """Load all Parquet files from the specified directory and return them as a list of DataFrames."""
    logging.info(f"Loading Parquet files from directory: {directory}")

    dataframes = []
    for filename in os.listdir(directory):
        if filename.endswith(".parquet"):
            logging.debug(f"Loading file: {filename}")
            df = pandas.read_parquet(os.path.join(directory, filename))
            dataframes.append(df)

    logging.info(f"Loaded {len(dataframes)} Parquet files.")
    return dataframes


def bulk_insert_records(con, df: pandas.DataFrame, file_name: str):
    """Insert multiple records into the database in a single query."""
    # Add the file_name column to the DataFrame
    df["file_name"] = file_name

    # Insert the entire DataFrame into DuckDB
    con.execute("INSERT INTO raw_taxi_trips SELECT * FROM df")


def file_already_imported(con, file_name: str) -> bool:
    """Check if a file has already been imported by checking the file_name column."""
    query = "SELECT COUNT(*) FROM raw_taxi_trips WHERE file_name = ?"
    result = con.execute(query, (file_name,)).fetchone()
    return result[0] > 0


def normalize_dataframe(
    df: pandas.DataFrame, field_map: Dict[str, str]
) -> pandas.DataFrame:
    """Vectorized normalization of the entire dataframe based on the precomputed field map."""
    # Rename columns based on the field map (vectorized)
    df_normalized = df.rename(columns=field_map)

    # Add missing columns with default values from TaxiTrip
    for field_obj in fields(TaxiTrip):
        if field_obj.name not in df_normalized.columns:
            df_normalized[field_obj.name] = field_obj.default

    return df_normalized


def bulk_insert_records(con, df: pandas.DataFrame, file_name: str):
    """Insert multiple records into the database in a single query."""
    # Add the file_name column to the DataFrame
    df["file_name"] = file_name

    # Insert in batches for efficiency
    for i in range(0, len(df), BATCH_SIZE):
        chunk = df.iloc[i : i + BATCH_SIZE]

        # Create a temporary DuckDB relation from the chunk
        chunk_rel = con.from_df(chunk)

        # Insert data from the relation into the table
        chunk_rel.insert_into("raw_taxi_trips")


def main():
    data_dir = "data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # Create a DuckDB connection and raw_taxi_trips table
    duckdb_file_path = os.path.join(data_dir, "taxi_trips.duckdb")
    con = duckdb.connect(duckdb_file_path)
    create_table_from_dataclass(con, "raw_taxi_trips", TaxiTrip)

    # Precompute the field mappings for normalization
    field_map = precompute_field_mappings()

    # Load all parquet files
    parquet_filenames = [f for f in os.listdir(data_dir) if f.endswith(".parquet")]

    if not parquet_filenames:
        logging.info(
            "No Parquet files found in the data directory. Exiting the application."
        )
        time.sleep(5)
        sys.exit(0)

    # Process each file
    for file_name in parquet_filenames:
        # Check if the file has already been imported
        if file_already_imported(con, file_name):
            logging.info(f"Skipping {file_name}, already imported.")
            continue

        logging.info(f"Processing {file_name}...")

        # Load the parquet file into a DataFrame only if not already imported
        file_path = os.path.join(data_dir, file_name)
        df = pandas.read_parquet(file_path)

        # Vectorized normalization of the entire DataFrame
        df_normalized = normalize_dataframe(df, field_map)

        # Bulk insert the normalized DataFrame in batches
        bulk_insert_records(con, df_normalized, file_name)


if __name__ == "__main__":
    main()
