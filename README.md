# Spotify Data ELT Pipeline Project

## Project Overview

This project develops a robust ELT (Extract, Load, Transform) pipeline to handle Spotify data efficiently. Utilizing Astronomer and Apache Airflow, the pipeline orchestrates daily tasks to extract data from the Spotify API, load it into an AWS S3 data lake, transform it for analytical readiness, and prepare it for reporting and visualization.

## Infrastructure and Pipeline Setup

### Infrastructure Provisioning with Terraform

The infrastructure for the project is provisioned using Terraform, ensuring consistent and reproducible setup:

- **Data Lake**: An S3 bucket is configured to store both raw and transformed data, organized to maintain clarity between raw data and data ready for analysis.

### Pipeline Orchestration with Astronomer and Airflow

Daily orchestrated tasks include:

1. **Data Extraction**: Fetches raw data from the Spotify API.
2. **Data Loading**: Stores the raw data as JSON files in the designated S3 bucket.
3. **Data Transformation**: Transforms the raw data into a format suitable for analysis and stores it as Parquet files in S3.
4. **External Table Creation**: Sets up external tables pointing to the Parquet files in S3, making the data queryable.
5. **Further Data Transformation**: Processes the data stored in S3 to create enriched datasets in the development/production environments for deeper analysis.

## Current Capabilities

- **Automated Data Extraction and Loading**: Ensures systematic fetching and storage of Spotify data.
- **Scheduled Transformations**: Transforms data on a predefined schedule, ensuring readiness for analysis.
- **Infrastructure as Code (IaC)**: Uses Terraform for repeatable and reliable infrastructure setup.

## Upcoming Steps in Data Transformation

### Data Transformation and Enrichment

- **Transformation Process**: Scripts will be developed to convert raw JSON data into Parquet format, enhancing query performance and reducing storage costs.
- **Data Enrichment**: Additional data sources will be integrated, performing joins, aggregations, and other transformations to enrich the data.

### Testing and Documentation

- **Unit Tests**: Unit tests will be implemented for transformation scripts to ensure transformation logic accuracy and reliability.
- **Documentation**: Detailed documentation will be provided for each transformation step and the data structure of both raw and transformed datasets.

### Reporting and Visualization

- **Data Visualization**: Dashboards and reports using BI tools will be developed to visualize key metrics and trends from the transformed data.
- **Data Accessibility**: Ensure that transformed data is easily accessible to both technical and non-technical stakeholders.

## Long-Term Vision

- **Expand Data Sources**: More data sources will be incorporated to provide a holistic view of music industry trends.
- **Machine Learning Integration**: Leverage transformed data for predictive analytics and machine learning models.
- **Real-Time Data Processing**: Move towards real-time data processing to provide up-to-date insights.
