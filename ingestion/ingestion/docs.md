# Spotify Data Pipeline Project Documentation

## Overview

This project implements a data pipeline for fetching, processing, and storing Spotify data. The pipeline consists of three main stages: Raw, Bronze, and Silver. Each stage is responsible for different aspects of data handling, from initial API retrieval to structured storage and transformation.

The pipeline is designed to be executed as part of an Airflow DAG, with each stage represented by a separate Python script. The scripts work together to create a robust ETL (Extract, Transform, Load) process for Spotify data.

## Pipeline Stages

### 1. Raw Stage (raw.py)

The raw stage is responsible for interacting with the Spotify API, fetching data, and storing it in its original format.
Key components:

- SpotifyAPIClient: Handles authentication and API requests to Spotify.
- DataParser: Parses JSON data returned from the API.
- DataSaver: Saves data locally and to AWS S3.
- Ingestor: Orchestrates the entire process of fetching, parsing, and saving data.

### Main functions:

- Authenticate with Spotify API
- Search for tracks, artists, or playlists
- Parse and validate JSON responses
- Save raw data locally and to S3

### 2. Bronze Stage (bronze.py)
The bronze stage takes the raw data stored in S3 and performs initial transformations and structuring.

### Key components:

- DataManager: Manages data transformation and storage operations.
- Ingestor: Orchestrates the bronze stage processes.

### Main functions:

- Load data from S3 parquet files
- Create structured tables in DuckDB
- Transform and clean data
- Save processed data locally, to S3, and to MotherDuck

### 3. Silver Stage (silver.py)

The silver stage further refines the data, creating a more polished and query-ready dataset.

#### Key components:

- DataManager: Handles data operations for the silver stage.
- Ingestor: Orchestrates the silver stage processes.

#### Main functions:

- Create refined tables from bronze data
- Apply additional transformations and data quality checks
- Save processed data locally, to S3, and to MotherDuck in an optimized format

## Detailed Component Descriptions

### SpotifyAPIClient (raw.py)
The SpotifyAPIClient class is responsible for all interactions with the Spotify API. It handles authentication, token refresh, and making API requests.

Key methods:

- refresh_access_token(): Obtains a new access token from Spotify.
- _make_request(): Generic method for making API requests.
- search(): Performs searches for tracks, artists, or playlists with optional genre filtering.

### DataParser (raw.py)
The DataParser class provides utility methods for parsing JSON data returned from the Spotify API.

Key methods:

- parse_json_data(): Parses JSON strings into Python objects, handling potential errors.

### DataSaver (raw.py)
The DataSaver class manages the storage of data both locally and in AWS S3.

Key methods:

- save_local(): Saves data to the local file system.
- save_s3(): Uploads data to an AWS S3 bucket.

### DataManager (bronze.py and silver.py)
The DataManager class is central to both the bronze and silver stages, handling data loading, transformation, and storage operations.
Key methods:

- load_and_transform_data(): Loads data from S3 and applies initial transformations.
- process_data(): Processes loaded data based on its structure.
- handle_playlist(), handle_tracks(), handle_album(), handle_artists(): Specialized methods for processing different types of Spotify data.
- insert_data(): Inserts processed data into DuckDB tables.
- save_to_local(), save_to_s3(), save_to_md(): Methods for saving data to different storage systems.

### Other Manager Classes

In this pipeline, three manager classes are used to handle different aspects of the data processing and storage operations: `DuckDBManager`, `AWSManager`, and `MotherDuckManager`. Each of these classes is designed to manage specific tasks, ensuring that the pipeline runs smoothly and efficiently.

#### DuckDBManager (manager.py)

The `DuckDBManager` class manages the connection to DuckDB and executes SQL queries. It is responsible for creating and maintaining the database connection and executing various queries needed throughout the pipeline.

**Key methods:**

- **create_connection()**: Establishes a connection to DuckDB and sets the S3 endpoint.
- **execute_query(query: str, params=None)**: Executes a given SQL query with optional parameters.

#### AWSManager (manager.py)

The `AWSManager` class manages AWS credentials and operations, particularly focusing on integrating AWS S3 with DuckDB. It ensures that the pipeline can access and utilize AWS resources securely and efficiently.

**Key methods:**

- **create_s3_client(aws_region: str, aws_access_key: str, aws_secret_access_key: str)**: Creates a boto3 S3 client with the given credentials.
- **load_credentials(aws_region: str, aws_access_key: str, aws_secret_access_key: str)**: Loads AWS credentials into DuckDB settings.

#### MotherDuckManager (manager.py)

The `MotherDuckManager` class handles the connection to MotherDuck, a data management platform, ensuring that the pipeline can integrate and utilize MotherDuck services effectively.

**Key methods:**

- **connect(motherduck_token: str)**: Connects to MotherDuck using the provided token.

### Ingestor (all stages)
The Ingestor class orchestrates the entire process for each stage, coordinating between different components.

Key method:

- execute(): Runs the main process for the respective stage, calling other components as needed.

### Data Flow

- Raw Stage: Fetches data from Spotify API and stores it in S3.
- Bronze Stage: Loads raw data from S3, performs initial structuring, and saves to DuckDB, S3, and MotherDuck.
- Silver Stage: Further refines bronze data, applying additional transformations, and saves the polished dataset.

### Configuration and Environment

The project uses environment variables for configuration, loaded via dotenv. This includes API credentials, AWS settings, and database paths.

### Error Handling and Logging
Comprehensive error handling and logging are implemented throughout the project using the loguru library, ensuring robust operation and easy debugging.