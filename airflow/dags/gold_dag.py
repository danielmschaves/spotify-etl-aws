from datetime import datetime, timedelta
import os
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    "catchup": False
}

profile_config = ProfileConfig(profile_name="spotify_etl_aws",
                               target_name="dev",
                               profiles_yml_filepath="/usr/local/airflow/dags/dbt/spotify_etl_aws/profiles.yml")

project_config = ProjectConfig(dbt_project_path="/usr/local/airflow/dags/dbt/spotify_etl_aws")

OPERATOR_ARGS = {
    "install_deps": True,
    "env": {
        "HOME": "/usr/local/airflow/dags/dbt/spotify_etl_aws",
        "AWS_REGION": os.environ["AWS_REGION"],
        "AWS_ACCESS_KEY": os.environ["AWS_ACCESS_KEY"],
        "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
        "TRANSFORM_S3_PATH_INPUT": os.environ["TRANSFORM_S3_PATH_INPUT"],
        "TRANSFORM_S3_PATH_OUTPUT": os.environ["TRANSFORM_S3_PATH_OUTPUT"],
        "MOTHERDUCK_DATABASE": os.environ["MOTHERDUCK_DATABASE"]
    }
}

gold_dag = DbtDag(project_config=project_config,
                    operator_args=OPERATOR_ARGS,
                    profile_config=profile_config,
                    execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",),
                    default_args=default_args,
                    tags=['gold_ingestion'],
                    dag_id='gold_ingestion')



