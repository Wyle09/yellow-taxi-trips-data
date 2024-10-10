# Yellow Taxi Trip Distance Analysis
This project focuses on extracting and analyzing taxi trips over the 0.9 percentile in distance traveled from a dataset of New York City taxi trips stored in Parquet format.

## Problem Statement
Task: Extract all taxi trips that exceed the 0.9 percentile of distance traveled for any available Parquet files.

## Initial Considerations
1. **Which Parquet Files to Use?**
*  observed that the dataset contains files dating back to 2015. For this analysis, I focused on the 2023 and 2024 data.

2. **Relevant Columns:**
* The primary focus was on columns related to trip identification and distance:
* `vendor_id`
* `tpep_pickup_datetime`
* `tpep_dropoff_datetime`
* `pu_location_id`
* `do_location_id`
* `trip_distance`

3. **Data Quality Considerations:**
* I handled missing or invalid values (e.g., zero or null distances) by adding default values where necessary based on the Parquet schema.

4. Technology Choice:
* The solution is implemented using Python and DuckDB, packaged within Docker for ease of reproducibility.

## Steps to Solve the Problem
1. **Dataset and File Selection:**
* I initially reviewed the available Parquet files, which spanned multiple years, and decided to focus on the 2023 and 2024 data for this analysis.
* used the parq-client to inspect the schema of individual Parquet files to understand their structure and how the data is formatted:
```bash
parq data/yellow_tripdata_2023-01.parquet --schema
```

* Example schema inspection:
```bash
❯ parq data/yellow_tripdata_2023-01.parquet --schema

 # Schema 
 <pyarrow._parquet.ParquetSchema object at 0x10ea83a00>
required group field_id=-1 schema {
  optional int64 field_id=-1 VendorID;
  optional int64 field_id=-1 tpep_pickup_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int64 field_id=-1 tpep_dropoff_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional double field_id=-1 passenger_count;
  optional double field_id=-1 trip_distance;
  optional double field_id=-1 RatecodeID;
  optional binary field_id=-1 store_and_fwd_flag (String);
  optional int64 field_id=-1 PULocationID;
  optional int64 field_id=-1 DOLocationID;
  optional int64 field_id=-1 payment_type;
  optional double field_id=-1 fare_amount;
  optional double field_id=-1 extra;
  optional double field_id=-1 mta_tax;
  optional double field_id=-1 tip_amount;
  optional double field_id=-1 tolls_amount;
  optional double field_id=-1 improvement_surcharge;
  optional double field_id=-1 total_amount;
  optional double field_id=-1 congestion_surcharge;
  optional double field_id=-1 airport_fee;
}

❯ parq data/yellow_tripdata_2024-01.parquet --schema

 # Schema 
 <pyarrow._parquet.ParquetSchema object at 0x11921f5c0>
required group field_id=-1 schema {
  optional int32 field_id=-1 VendorID;
  optional int64 field_id=-1 tpep_pickup_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int64 field_id=-1 tpep_dropoff_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int64 field_id=-1 passenger_count;
  optional double field_id=-1 trip_distance;
  optional int64 field_id=-1 RatecodeID;
  optional binary field_id=-1 store_and_fwd_flag (String);
  optional int32 field_id=-1 PULocationID;
  optional int32 field_id=-1 DOLocationID;
  optional int64 field_id=-1 payment_type;
  optional double field_id=-1 fare_amount;
  optional double field_id=-1 extra;
  optional double field_id=-1 mta_tax;
  optional double field_id=-1 tip_amount;
  optional double field_id=-1 tolls_amount;
  optional double field_id=-1 improvement_surcharge;
  optional double field_id=-1 total_amount;
  optional double field_id=-1 congestion_surcharge;
  optional double field_id=-1 Airport_fee;
}

❯ parq data/yellow_tripdata_2024-07.parquet --schema
 # Schema 
 <pyarrow._parquet.ParquetSchema object at 0x110e63500>
required group field_id=-1 schema {
  optional int32 field_id=-1 VendorID;
  optional int64 field_id=-1 tpep_pickup_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int64 field_id=-1 tpep_dropoff_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int64 field_id=-1 passenger_count;
  optional double field_id=-1 trip_distance;
  optional int64 field_id=-1 RatecodeID;
  optional binary field_id=-1 store_and_fwd_flag (String);
  optional int32 field_id=-1 PULocationID;
  optional int32 field_id=-1 DOLocationID;
  optional int64 field_id=-1 payment_type;
  optional double field_id=-1 fare_amount;
  optional double field_id=-1 extra;
  optional double field_id=-1 mta_tax;
  optional double field_id=-1 tip_amount;
  optional double field_id=-1 tolls_amount;
  optional double field_id=-1 improvement_surcharge;
  optional double field_id=-1 total_amount;
  optional double field_id=-1 congestion_surcharge;
  optional double field_id=-1 Airport_fee;
}


❯ parq data/yellow_tripdata_2023-07.parquet --schema

 # Schema 
 <pyarrow._parquet.ParquetSchema object at 0x11003ab80>
required group field_id=-1 schema {
  optional int32 field_id=-1 VendorID;
  optional int64 field_id=-1 tpep_pickup_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int64 field_id=-1 tpep_dropoff_datetime (Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false));
  optional int64 field_id=-1 passenger_count;
  optional double field_id=-1 trip_distance;
  optional int64 field_id=-1 RatecodeID;
  optional binary field_id=-1 store_and_fwd_flag (String);
  optional int32 field_id=-1 PULocationID;
  optional int32 field_id=-1 DOLocationID;
  optional int64 field_id=-1 payment_type;
  optional double field_id=-1 fare_amount;
  optional double field_id=-1 extra;
  optional double field_id=-1 mta_tax;
  optional double field_id=-1 tip_amount;
  optional double field_id=-1 tolls_amount;
  optional double field_id=-1 improvement_surcharge;
  optional double field_id=-1 total_amount;
  optional double field_id=-1 congestion_surcharge;
  optional double field_id=-1 Airport_fee;
}
```
2. **Data Ingestion and Key Logic**
I developed a `Python` application to handle data loading, processing, and insertion into `DuckDB`. The application is designed to be modular, allowing flexibility in working with datasets across different Parquet files.

* **Key Components**
    * TaxiTrip `Dataclass`
        * A Python dataclass TaxiTrip is used to represent the taxi trip schema. This class is designed to normalize the different field names found in the Parquet files and provide default values for any missing data.
        * The dataclass also includes an aliasing mechanism to map inconsistent field names like `VendorID` and `vendor_id` to a single, normalized field name.
    * Mapping Python Types to `DuckDB`:
        * The program dynamically generates a `DuckDB` table schema based on the TaxiTrip dataclass, mapping Python types (like `int`, `str`, and `float`) to their DuckDB equivalents (`INT`, `STRING`, `DOUBLE`).
    * Data Normalization:
        * Once loaded, each DataFrame is normalized using a precomputed field map. This field map aligns the raw field names from the Parquet files with the fields defined in the `TaxiTrip` dataclass. It also ensures any missing columns are filled with appropriate default values.
    * Batch Processing and Insertion into DuckDB:
        * Data is inserted into `DuckDB` in batches to avoid performance bottlenecks when dealing with large datasets. A batch size of 1,000 records is used to optimize the speed of insertion.
        * The program also ensures no data is duplicated by checking if a file has already been imported based on the `file_name` field, preventing redundant imports.


3. **Key Fields for Analysis:**
* The main columns I focused on were:
    * `vendor_id`
    * `tpep_pickup_datetime`
    * `tpep_dropoff_datetime`
    * `pu_location_id`
    * `do_location_id`
    * `trip_distance`
* These columns formed the basis for determining the unique taxi trips and calculating the 0.9 percentile for trip distance.

4. **Data Cleaning and Deduplication:**
* During the analysis, I discovered that some records were duplicated based on minor variations in other fields like, total_amount, tolls_amount, improvement_surcharge, etc
* Example query to identify duplicates based on the key fields:
```sql
SELECT
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pu_location_id,
    do_location_id,
    trip_distance,
    count(*) AS cnt
FROM raw_taxi_trips
GROUP BY
    vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id, trip_distance
HAVING cnt > 1;
```
* I then used this information to create a table of unique taxi trips:
```sql
    CREATE TABLE IF NOT EXISTS unique_taxi_trips AS 
    SELECT
        vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        pu_location_id,
        do_location_id,
        MAX(trip_distance) AS trip_distance
    FROM raw_taxi_trips
    GROUP BY vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id;
```

5. **Percentile Calculation:**
* To find trips that exceeded the 0.9 percentile in trip distance, I used the following two-step process:
    * First, calculate the 0.9 percentile for trip_distance:
    ```sql
    WITH percentile_trip_distance AS (
        SELECT approx_quantile(trip_distance, 0.9) AS p90_trip_distance
        FROM unique_taxi_trips
    )
    ```
    * Then, filter the trips that exceeded this threshold:
    ```sql
    SELECT *
    FROM unique_taxi_trips
    WHERE trip_distance > (SELECT p90_trip_distance FROM percentile_trip_distance);
    ```
    * Example of results
    ```sql
        ┌───────────┬──────────────────────┬───────────────────────┬────────────────┬────────────────┬───────────────┐
        │ vendor_id │ tpep_pickup_datetime │ tpep_dropoff_datetime │ pu_location_id │ do_location_id │ trip_distance │
        │   int32   │       varchar        │        varchar        │     int32      │     int32      │    double     │
        ├───────────┼──────────────────────┼───────────────────────┼────────────────┼────────────────┼───────────────┤
        │         1 │ 2023-01-01 00:02:29  │ 2023-01-01 00:29:57   │            132 │            141 │          20.2 │
        │         1 │ 2023-01-01 00:02:47  │ 2023-01-01 00:24:21   │            138 │            144 │           9.1 │
        │         1 │ 2023-01-01 00:03:00  │ 2023-01-01 00:33:19   │            161 │            228 │          13.8 │
        │         1 │ 2023-01-01 00:04:45  │ 2023-01-01 00:45:38   │            161 │            228 │           9.7 │
        │         1 │ 2023-01-01 00:04:54  │ 2023-01-01 00:22:57   │            138 │            244 │           9.4 │
        │         1 │ 2023-01-01 00:05:17  │ 2023-01-01 00:27:47   │            137 │            130 │          13.2 │
        │         1 │ 2023-01-01 00:05:21  │ 2023-01-01 00:52:00   │            143 │            188 │          10.0 │
        │         1 │ 2023-01-01 00:07:31  │ 2023-01-01 00:35:46   │            132 │             22 │          19.0 │
        │         1 │ 2023-01-01 00:08:29  │ 2023-01-01 00:25:41   │            141 │            129 │          10.5 │
        │         1 │ 2023-01-01 00:09:02  │ 2023-01-01 00:31:45   │            132 │            233 │          16.7 │
        │         1 │ 2023-01-01 00:11:44  │ 2023-01-01 00:33:39   │            132 │            171 │          12.7 │
        │         1 │ 2023-01-01 00:11:49  │ 2023-01-01 00:40:04   │            132 │            107 │          17.2 │
        │         1 │ 2023-01-01 00:13:30  │ 2023-01-01 00:44:00   │            132 │            116 │          17.8 │
        │         1 │ 2023-01-01 00:14:07  │ 2023-01-01 00:53:40   │            249 │            243 │          10.9 │
        │         1 │ 2023-01-01 00:14:36  │ 2023-01-01 00:27:40   │            138 │            140 │           9.0 │
        │         1 │ 2023-01-01 00:15:05  │ 2023-01-01 00:44:14   │            132 │            188 │           9.9 │
        │         1 │ 2023-01-01 00:16:10  │ 2023-01-01 00:41:26   │            132 │            262 │          19.4 │
        │         1 │ 2023-01-01 00:16:44  │ 2023-01-01 00:42:40   │            162 │            208 │          11.7 │
        │         1 │ 2023-01-01 00:17:10  │ 2023-01-01 00:42:40   │            236 │             97 │           9.1 │
        │         1 │ 2023-01-01 00:18:16  │ 2023-01-01 01:00:37   │            132 │            143 │          18.0 │
        │         · │          ·           │          ·            │             ·  │              · │            ·  │
        │         · │          ·           │          ·            │             ·  │              · │            ·  │
        │         · │          ·           │          ·            │             ·  │              · │            ·  │
        │         6 │ 2024-05-30 06:05:55  │ 2024-05-30 06:05:28   │            265 │             42 │         10.08 │
        │         6 │ 2024-05-30 06:05:59  │ 2024-05-30 06:05:26   │            265 │             68 │         14.93 │
        │         6 │ 2024-05-30 07:05:13  │ 2024-05-30 07:05:29   │            265 │            107 │          9.28 │
        │         6 │ 2024-05-30 10:05:08  │ 2024-05-30 10:05:50   │            265 │            218 │          9.07 │
        │         6 │ 2024-05-30 21:05:10  │ 2024-05-30 22:05:49   │            265 │            215 │         14.55 │
        │         6 │ 2024-05-30 22:05:00  │ 2024-05-30 23:05:38   │            265 │            202 │         16.43 │
        │         6 │ 2024-05-30 23:05:44  │ 2024-05-31 01:05:48   │            265 │            235 │          9.26 │
        │         6 │ 2024-05-31 01:05:33  │ 2024-05-31 01:05:53   │            265 │            137 │          13.9 │
        │         6 │ 2024-05-31 02:05:16  │ 2024-05-31 03:05:12   │            265 │            218 │         16.87 │
        │         6 │ 2024-05-31 04:05:01  │ 2024-05-31 05:05:25   │            265 │            114 │         16.49 │
        │         6 │ 2024-05-31 07:05:31  │ 2024-05-31 07:05:41   │            265 │             41 │          13.1 │
        │         6 │ 2024-05-31 12:05:59  │ 2024-05-31 13:05:41   │            265 │             41 │         10.78 │
        │         6 │ 2024-05-31 14:05:10  │ 2024-05-31 15:05:54   │            265 │            197 │         10.31 │
        │         6 │ 2024-05-31 14:05:35  │ 2024-05-31 15:05:40   │            265 │             29 │         19.24 │
        │         6 │ 2024-05-31 15:05:13  │ 2024-05-31 16:05:37   │            265 │            210 │         19.62 │
        │         6 │ 2024-06-01 01:06:31  │ 2024-06-01 02:06:29   │            265 │            222 │         15.47 │
        │         6 │ 2024-06-01 10:06:12  │ 2024-06-01 11:06:19   │            265 │            229 │         12.34 │
        │         6 │ 2024-06-02 08:06:36  │ 2024-06-02 09:06:02   │            265 │            116 │         19.17 │
        │         6 │ 2024-06-03 08:06:56  │ 2024-06-03 09:06:30   │            265 │            170 │         21.64 │
        │         6 │ 2024-06-03 14:06:47  │ 2024-06-03 15:06:12   │            265 │            124 │         12.88 │
        ├───────────┴──────────────────────┴───────────────────────┴────────────────┴────────────────┴───────────────┤
        │ 6122942 rows (40 shown)                                                                          6 columns │
        └────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
    ```

6. **Verification**
* After deduplication and percentile calculation, I verified the counts of unique records and filtered records to ensure the analysis was accurate:
    * Checked the counts from the raw table & the unique table 

    ```sql
    -- Verify counts with raw table & unique table
    SELECT COUNT(*) FROM unique_taxi_trips;
    ┌──────────────┐
    │ count_star() │
    │    int64     │
    ├──────────────┤
    │     61115175 │
    └──────────────┘

    SELECT COUNT(*)
    FROM (
        SELECT DISTINCT vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pu_location_id, do_location_id
        FROM raw_taxi_trips
    ) AS distinct_trips;
    ┌──────────────┐
    │ count_star() │
    │    int64     │
    ├──────────────┤
    │     61115175 │
    ```
    * Raw table total records
    ```sql
    SELECT count(*) FROM raw_taxi_trips;

    ┌──────────────┐
    │ count_star() │
    │    int64     │
    ├──────────────┤
    │     61719222 │
    └──────────────┘
    ```

# How to Run the Project
## Prerequisites
Before you begin, ensure that you have the following installed on your system:
1. **Docker and Docker Compose** (used for containerizing and running the project):
* [Docker Engine](https://docs.docker.com/engine/install/)

2. **DuckDB CLI:**
* Follow the installation instructions for your platform here: [DuckDB Installation Guide](https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=macos&download_method=package_manager)

## Steps to Run the Project
1. **Clone the Repository:** Clone the project to your local machine:
```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

2. **Prepare the Data:** Ensure you have placed the required Parquet files inside the data/ directory before starting the container. You can update the file paths as needed.
* The python script will create a data dir if it does not exist. You should still place the Parquet files into this folder manually after creation.

3. **Start Docker Containers:** Use Docker Compose to build and run the necessary containers.
```bash
docker-compose up --build
```

4. **Access the DuckDB Database:** After the containers are running, open a new terminal window and run the DuckDB CLI to interact with the database.
```bash
duckdb data/taxi_trips.duckdb
```

5. **Running the Application:** The Python application inside the Docker container will handle data ingestion and processing. Once the container is up and running, data will be processed and inserted into DuckDB in batches automatically. The container should exit once the ingestion is complete. 
* **Stopping the Containers:** (If needed) When you're done with the project, stop and remove the running containers:
```bash
docker-compose down
```
