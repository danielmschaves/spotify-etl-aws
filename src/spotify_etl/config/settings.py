"""Configuration settings using Pydantic for validation."""

from pydantic import Field
try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ImportError:
    # Fallback for older Pydantic versions
    from pydantic import BaseSettings
    SettingsConfigDict = None


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    # Spotify API Configuration
    api_base_url: str = Field(default="https://api.spotify.com/v1/", description="Spotify API base URL")
    client_id: str = Field(..., description="Spotify API client ID")
    client_secret: str = Field(..., description="Spotify API client secret")

    # AWS Configuration
    aws_access_key: str = Field(..., description="AWS access key ID")
    aws_secret_access_key: str = Field(..., description="AWS secret access key")
    aws_region: str = Field(default="us-east-2", description="AWS region")
    raw_bucket: str = Field(..., description="S3 bucket for raw data")
    bronze_bucket: str = Field(..., description="S3 bucket for bronze data")
    bronze_s3_path: str = Field(..., description="S3 path for bronze data")
    silver_s3_path: str = Field(..., description="S3 path for silver data")

    # Data Paths
    table_name: str = Field(default="spotify_data", description="Default table name")
    table_path: str = Field(default="data/raw/", description="Local path for raw data")
    bronze_local_path: str = Field(default="data/bronze/", description="Local path for bronze data")
    silver_local_path: str = Field(default="data/silver/", description="Local path for silver data")

    # Database Configuration
    local_database: str = Field(default="memory", description="Local DuckDB database name")
    remote_database: str = Field(default="playlist", description="Remote database name")
    bronze_schema: str = Field(default="bronze", description="Bronze schema name")
    silver_schema: str = Field(default="silver", description="Silver schema name")

    # MotherDuck Configuration
    motherduck_token: str = Field(..., description="MotherDuck authentication token")

    # dbt Configuration
    transform_s3_path_input: str = Field(..., description="S3 path for dbt input")
    transform_s3_path_output: str = Field(..., description="S3 path for dbt output")
    motherduck_database: str = Field(..., description="MotherDuck database for dbt")

    class Config:
        """Pydantic configuration."""

        env_file = ".env"
        case_sensitive = False

    if SettingsConfigDict:
        model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
