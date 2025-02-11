# data_engineer_task
# Incremental Data Processing with Airflow and BigQuery

## Introduction
This project implements an incremental data processing pipeline using Apache Airflow and Google BigQuery. It supports the following functionalities:

- **Incrementally loading new or updated records** from BigQuery tables based on timestamps.
- **Processing JSON data** and inserting it into BigQuery tables.
- **Handling epoch timestamp conversion** for structured storage.

The system is designed to be modular and scalable, allowing integration with additional data sources and transformations in the future.

## Why I Made These Choices

### 1. ETL Loading Design
The project follows a data pipeline approach where each component serves a specific purpose:

- `incremental_load.py`: Implements an Airflow DAG to fetch new or updated records from BigQuery tables.
- `process_json_epoch_created_at.py`: Reads a JSON file, processes epoch timestamps, and inserts structured data into BigQuery.

This separation ensures flexibility and simplifies debugging while maintaining clear responsibilities.

### 2. Folder Structure and Naming
The project structure adheres to clean architecture principles:

- **config/**: Centralized configuration management for BigQuery connections and dataset settings.
- **scripts/**: Contains processing logic for incremental loading and JSON handling.
- **dags/**: Defines Airflow DAGs for scheduling and automation.
- **logs/**: Stores logs for debugging and monitoring purposes.

This structure ensures separation of concerns, making the codebase easy to maintain and extend.

- **Note**: The JSON you provided is not valid. Here are the main issues:

**String Quotes**:
In JSON, all strings (both keys and values) must be enclosed in double quotes ("). In your snippet, many strings are enclosed in single quotes (').

**Boolean Values**:
JSON uses lowercase true and false for boolean values. Your snippet uses Python’s True and False.

(Optional) The next_offset Value:
The value for "next_offset" is given as a string that looks like an array ('["1698915678000","63969095"]'). Depending on your intent, you might want this to be an actual JSON array rather than a string.

etl.json is the corrected JSON with all keys and string values in double quotes, booleans in lowercase, and with "next_offset" converted into an actual array

### 3. Incremental Loading Strategy
A time-based incremental strategy is implemented:

- Uses BigQuery's `INFORMATION_SCHEMA.COLUMNS` to check if a table has `created_at` or `updated_at` columns.
- Queries only records that have been created or updated within the last 60 minutes.
- Prevents unnecessary full-table scans, improving efficiency.

### 4. Error Handling
Error handling ensures meaningful feedback is provided during execution:

- Validates BigQuery connections before processing.
- Catches and logs errors related to missing columns or invalid JSON data.
- Prints detailed logs to assist debugging.

## Incremental Load
This project includes an Airflow DAG for automated scheduling and JSON-based processing. Below are the key components:

### 1. Airflow DAG (`incremental_load.py`)
Description: Loads new or updated records from BigQuery every hour.

#### Workflow:
1. Airflow scheduler triggers the DAG.
2. It checks each table in `TABLES` (`Customers`, `Subscriptions`).
3. Queries records where `created_at` or `updated_at` is within the last 60 minutes.
4. Outputs the number of new records found.

### 2. JSON Processing Script (`process_json_epoch_created_at.py`)
Description: Reads `etl.json`, converts epoch timestamps, and loads data into BigQuery.

#### Workflow:
1. Reads `etl.json` from the working directory.
2. Converts epoch timestamps into ISO UTC format.
3. Validates and structures customer and subscription data.
4. Inserts cleaned data into BigQuery tables (`customers`, `subscriptions`).

## How to Run the Project

### Prerequisites
Ensure you have the following installed:
- Apache Airflow
- Google Cloud SDK
- `google-cloud-bigquery` Python library

### Running the Airflow DAG
1. Place `incremental_load.py` in the Airflow DAGs folder.
2. Start Airflow Scheduler:
   ```sh
   airflow scheduler
   ```
3. Start Airflow Webserver:
   ```sh
   airflow webserver
   ```
4. Enable the DAG from the Airflow UI.

### Running the JSON Processing Script
1. Ensure Google Cloud SDK is authenticated.
2. Run the script:
   ```sh
   python3 process_json_epoch_created_at.py
   ```

## Future Improvements
This project meets current requirements but could be enhanced further:

### Enhancements
1. **Optimize Queries**: Improve incremental logic to support different data sources.
2. **Extend Logging**: Integrate with monitoring tools for better visibility.
3. **Automate JSON Processing**: Convert it into an Airflow task for consistency.
