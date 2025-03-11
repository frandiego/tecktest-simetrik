ARG PYTHON_VERSION=3.12

FROM python:${PYTHON_VERSION}-slim-bookworm 

# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh
RUN curl https://install.duckdb.org | sh


# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"


WORKDIR /app
RUN uv init --python ${PYTHON_VERSION} --bare
RUN uv add \
    pandas==2.2.3 \
    loguru==0.7.3 \
    typer==0.15.2 \
    click==8.1.8 \
    requests==2.32.3 \
    dbt-core==1.9.3 \
    dbt-duckdb==1.9.2 \
    harlequin==2.1.0  \
    dagster==1.10.4 \
    dagster-webserver==1.10.4 \
    dagster-duckdb==0.26.4 \
    dagster-duckdb-pandas==0.26.4  \
    dagit==1.10.4 \
    polars==1.24.0  \
    dagster-duckdb-polars==0.26.4

ADD . .
EXPOSE 3000

