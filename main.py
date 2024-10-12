import logging
import os
import sys
import time
from dataclasses import dataclass, field, fields
from typing import Dict, get_type_hints

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)

# Map Python types to DuckDB types
TYPE_MAPPING = {
    int: "INT",
    str: "STRING",
    float: "DOUBLE",
    bool: "BOOLEAN",
}


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
    logging.debug(f"Creating table {table_name} from dataclass {dataclass.__name__}")
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

    logging.debug(f"Executing SQL to create table: {create_table_sql}")
    # Execute the dynamically generated SQL
    con.execute(create_table_sql)


def precompute_field_mappings() -> dict:
    """Pre-compute a mapping from raw file field names (including all aliases) to normalized dataclass field names."""
    logging.debug("Precomputing field mappings for TaxiTrip dataclass")
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

    logging.debug(f"Field mappings: {field_mapping}")
    return field_mapping


def file_already_imported(con, file_name: str) -> bool:
    """Check if a file has already been imported by checking the file_name column."""
    logging.debug(f"Checking if file {file_name} has already been imported")
    query = "SELECT COUNT(*) FROM raw_taxi_trips WHERE file_name = ?"
    try:
        result = con.execute(query, (file_name,)).fetchone()
        if result and len(result) > 0:
            logging.debug(
                f"File {file_name} import status: {'Already imported' if result[0] > 0 else 'Not imported'}"
            )
            return result[0] > 0
        else:
            logging.error(
                f"Unexpected result format when checking file import status for {file_name}: {result}"
            )
            return False
    except Exception as e:
        logging.error(f"E")

def get_parquet_columns(con, file_path: str) -> list:
    """Retrieve the list of columns from a Parquet file."""
    try:
        parquet_columns = con.execute(f"SELECT * FROM read_parquet('{file_path}') LIMIT 0").fetchdf().columns
        return list(parquet_columns)
    except Exception as e:
        logging.error(f"Error retrieving columns from {file_path}: {e}")
        return []

def generate_select_statement(field_map: Dict[str, str], file_path: str, parquet_columns: list) -> str:
    """Generate the SELECT statement for mapping raw fields to normalized fields, handling missing columns."""
    select_clause = []
    for field_obj in fields(TaxiTrip):
        normalized_field = field_obj.name
        raw_field = field_map.get(normalized_field, normalized_field)
        default_value = field_obj.default

        match default_value:
            case str() if default_value:
                default_value_str = f"'{default_value}'"
            case str():
                default_value_str = "NULL"
            case int() | float() | bool():
                default_value_str = str(default_value)
            case _:
                default_value_str = "NULL"

        # Match-case for handling column presence in parquet_columns
        match raw_field in parquet_columns:
            case True:
                # Column exists in the parquet file, use COALESCE
                select_clause.append(f"COALESCE({raw_field}, {default_value_str})")
            case False:
                # Column does not exist, use the default value directly
                select_clause.append(f"{default_value_str} AS {normalized_field}")

    # Add file_name for tracking
    select_clause.append(f"'{os.path.basename(file_path)}' AS file_name")

    return ', '.join(select_clause)


def generate_insert_statement(select_clause: str, file_path: str) -> str:
    """Generate the INSERT statement for inserting records from a Parquet file."""
    return f"INSERT INTO raw_taxi_trips SELECT {select_clause} FROM read_parquet('{file_path}')"


def bulk_insert_records_from_parquet(con, file_path: str, field_map: Dict[str, str]):
    """Efficiently insert multiple records directly from a Parquet file with proper field mapping."""
    logging.info(f"Bulk inserting records from Parquet file: {file_path}")
    try:
        # Retrieve the columns from the Parquet file
        parquet_columns = get_parquet_columns(con, file_path)

        # Generate the SELECT statement to map the raw fields to normalized fields
        select_clause = generate_select_statement(field_map, file_path, parquet_columns)

        # Create the SQL for inserting with field mapping
        insert_sql = generate_insert_statement(select_clause, file_path)

        # Execute the insertion SQL
        con.execute(insert_sql)
        logging.info(f"Successfully inserted records from {file_path}")
    except Exception as e:
        logging.error(f"Error inserting records from {file_path}: {e}")


def main():
    data_dir = "data"
    logging.info(f"Starting data ingestion process, data directory: {data_dir}")
    if not os.path.exists(data_dir):
        logging.info(f"Data directory {data_dir} does not exist, creating it.")
        os.makedirs(data_dir)

    # Create a DuckDB connection and raw_taxi_trips table
    duckdb_file_path = os.path.join(data_dir, "taxi_trips.duckdb")
    logging.info(f"Connecting to DuckDB at path: {duckdb_file_path}")
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

    logging.info(f"Found {len(parquet_filenames)} Parquet files to process.")

    # Process each file serially for debugging purposes
    for file_name in parquet_filenames:
        if not file_already_imported(con, file_name):
            bulk_insert_records_from_parquet(
                con, os.path.join(data_dir, file_name), field_map
            )
        else:
            logging.info(f"Skipping {file_name}, already imported.")

    logging.info("Data ingestion process completed.")


if __name__ == "__main__":
    main()
