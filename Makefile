-include .env
export

install:
	poetry install && poetry shell

airflow-start:
	cd airflow && astro dev start

airflow-stop:
	cd airflow && astro dev stop

pipeline:
	astro deploy