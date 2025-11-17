"""Gold layer DAG using Cosmos for dbt transformations."""

from datetime import datetime, timedelta
import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
try:
    from cosmos.config import RenderConfig, SelectConfig, TestBehavior
except ImportError:
    # Fallback for older Cosmos versions
    RenderConfig = None
    SelectConfig = None
    TestBehavior = None


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "catchup": False,
}

# Profile configuration for dbt
profile_config = ProfileConfig(
    profile_name="spotify_etl_aws",
    target_name="dev",
    profiles_yml_filepath="/usr/local/airflow/dags/dbt/spotify_etl_aws/profiles.yml",
)

# Project configuration - point to the single source of truth dbt project
project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dags/dbt/spotify_etl_aws",
    models_relative_path="models",
)

# Render configuration for granular task generation (if available)
render_config = None
if RenderConfig:
    render_config = RenderConfig(
        emit_datasets=True,  # Better observability with datasets
        test_behavior=TestBehavior.AFTER_EACH if TestBehavior else None,  # Run tests after each model
    )

# Select configuration for model selection (if available)
select_config = None
if SelectConfig:
    select_config = SelectConfig(
        paths=["models/marts"],  # Only run marts models in gold layer
    )

# Operator arguments
OPERATOR_ARGS = {
    "install_deps": True,
    "env": {
        "HOME": "/usr/local/airflow/dags/dbt/spotify_etl_aws",
        "AWS_REGION": os.environ.get("AWS_REGION", ""),
        "AWS_ACCESS_KEY": os.environ.get("AWS_ACCESS_KEY", ""),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        "TRANSFORM_S3_PATH_INPUT": os.environ.get("TRANSFORM_S3_PATH_INPUT", ""),
        "TRANSFORM_S3_PATH_OUTPUT": os.environ.get("TRANSFORM_S3_PATH_OUTPUT", ""),
        "MOTHERDUCK_DATABASE": os.environ.get("MOTHERDUCK_DATABASE", ""),
    },
}

# Create the DAG using Cosmos
dag_kwargs = {
    "project_config": project_config,
    "operator_args": OPERATOR_ARGS,
    "profile_config": profile_config,
    "execution_config": ExecutionConfig(
        dbt_executable_path=f"{os.environ.get('AIRFLOW_HOME', '/usr/local/airflow')}/dbt_venv/bin/dbt",
    ),
    "default_args": default_args,
    "tags": ["gold_ingestion", "dbt"],
    "dag_id": "gold_ingestion",
    "schedule_interval": "@daily",
}

# Add optional configs if available
if render_config:
    dag_kwargs["render_config"] = render_config
if select_config:
    dag_kwargs["select_config"] = select_config

gold_dag = DbtDag(**dag_kwargs)
