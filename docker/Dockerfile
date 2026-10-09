FROM apache/airflow:3.3.2

# docker-compose bind-mounts the repository over /opt/provider at run time. Because the
# install is editable and points at that same path, code changes show up without a rebuild.
COPY --chown=airflow:root pyproject.toml README.md /opt/provider/
COPY --chown=airflow:root serpapi_provider /opt/provider/serpapi_provider
COPY --chown=airflow:root replay /opt/provider/replay
RUN pip install --no-cache-dir -e "/opt/provider[dev]"
