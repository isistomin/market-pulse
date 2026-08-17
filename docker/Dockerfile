FROM apache/airflow:3.3.1

# Airflow 3 dropped pandas from the core dependencies, so the ingestion imports
# have to be baked in. Extending the image keeps installs at build time instead of
# reinstalling on every container start, which is what _PIP_ADDITIONAL_REQUIREMENTS does.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt
