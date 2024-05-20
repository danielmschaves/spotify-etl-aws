# Spotify Data ELT Pipeline Project

![spotify.png](https://github.com/danielmschaves/spotify-etl-aws/blob/main/ingestion/img/spotify.png?raw=true)

## About the Project

This project follows the "poor man's data lake" concept, leveraging DuckDB, Motherduck, dbt, and Airflow to process data from the Spotify API. The primary focus is on playlists, tracks, albums, and artists. The goal is to build a robust ELT (Extract, Load, Transform) pipeline to efficiently handle Spotify data, ensuring it is ready for analysis and visualization. The proposed solution leverages processing and converting data into Parquet files that are then stored in S3 for sequential stage processing.


## Table of Contents
- [Problem Statement](#problem-statement)
- [Data Pipeline Overview](#data-pipeline-overview)
- [Data Pipeline Architecture](#data-pipeline-architecture)
- [Technologies](#technologies)
- [ELT Steps](#elt-steps)
- [Conclusion](#conclusion)

## Problem Statement

* ### ***Data***:
Data obtained using the Spotify API, containing information from popular global playlists, including tracks, albums, and artists.

* ### ***Proposed Solution***:
A daily scheduled batch extraction of playlist data from Spotify, transformed and made analytics-ready.

## Data Pipeline Overview

The pipeline orchestrates daily tasks to extract data from the Spotify API, load it into an AWS S3 data lake, transform it for analytical readiness, and prepare it for reporting and visualization.

![pipeline.png](https://github.com/danielmschaves/spotify-etl-aws/blob/main/ingestion/img/pipeline.png?raw=true)

## Data Pipeline Architecture

### Raw Stage
- **Purpose**: Store raw data extracted from the Spotify API.
- **Data Format**: JSON.
- **Storage**: AWS S3 (Raw Bucket).

### Bronze Stage
- **Purpose**: Initial transformation of raw data to a more structured format.
- **Data Format**: Parquet.
- **Storage**: AWS S3 (Bronze Bucket).
- **Tool**: DuckDB.

### Silver Stage
- **Purpose**: Further transformation of the data for analysis.
- **Data Format**: Parquet.
- **Storage**: AWS S3 (Silver Bucket).
- **Tool**: DuckDB.

### Gold Stage
- **Purpose**: Final transformation to an analytics-ready state.
- **Data Format**: Parquet.
- **Storage**: AWS S3 (Gold Bucket).
- **Tool**: dbt.

### Infrastructure Provisioning with Terraform

The infrastructure is provisioned using Terraform, ensuring consistent and reproducible setup:

- **S3 Bucket**: S3 buckets are configured to store both raw and transformed data, organized to maintain clarity between raw data and data ready for analysis.

### Pipeline Orchestration with Astronomer and Airflow

Daily orchestrated tasks include:

1. **Data Extraction**: Fetches raw data from the Spotify API.
2. **Data Loading**: Stores the raw data as JSON files in the designated S3 bucket.
3. **Data Transformation**: Transforms the raw data into a format suitable for analysis and stores it as Parquet files in S3.
4. **External Table Creation**: Sets up external tables pointing to the Parquet files in S3, making the data queryable.
5. **Further Data Transformation**: Processes the data stored in S3 to create enriched datasets in the development/production environments for deeper analysis.

## Technologies

- **API**: Spotify
- **Cloud**: AWS
- **Infrastructure as Code**: Terraform
- **Workflow Orchestration**: Astronomer + Airflow
- **Data Warehouse**: Motherduck
- **Data Transformation**: DBT
- **Data Visualization**: Power BI
- **Virtual Environment**: Poetry
- **CI/CD**: Git
- **Container**: Docker
- **Storage**: Amazon S3 and DuckDB

## ELT Steps

1. **A Project is created on GitHub**:
   - Version control and collaboration via GitHub.
   
2. **Infrastructure for the Project is created using Terraform**:
   - **Data Lake**: S3 Buckets where the raw and cleaned data will be stored.

3. **The Pipeline for ELT is created and is scheduled for daily execution**:
   - **Orchestrated via Astronomer + Airflow**, which does the following tasks:
     - **Extracts raw data** from source via Spotify API.
     - **Loads raw data** as JSON files to S3 Bucket.
     - **Cleans the raw data** using DuckDB.
     - **Loads the cleaned data** as Parquet files to S3.
     - **Creates External table** in the Datasets in Motherduck by pulling data from S3.
     - **Transforms Data from S3** using dbt-core and creates the following in the dev/prod Dataset (along with Tests and Documentation):
       - Transformed Data from Motherduck is used for **Reporting and Visualization** using Power BI to produce Dashboards.

## Conclusion

This project demonstrates the creation of an efficient and automated ELT pipeline using a combination of modern data engineering tools and techniques. The pipeline ensures data from the Spotify API is systematically extracted, transformed, and loaded into an analytical-ready state, facilitating comprehensive reporting and visualization.    