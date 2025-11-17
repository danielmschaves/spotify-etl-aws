FROM quay.io/astronomer/astro-runtime:11.3.0

# Install dbt into a venv to avoid package dependency conflicts
WORKDIR "/usr/local/airflow"
COPY dbt-requirements.txt ./
RUN python -m virtualenv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

# Set Python path to include src directory
ENV PYTHONPATH=/usr/local/airflow/src:${PYTHONPATH}

