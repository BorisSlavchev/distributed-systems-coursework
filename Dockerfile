FROM python:3.14.4-slim AS base

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PATH="/opt/venv/bin:$PATH"
WORKDIR /app
RUN python -m venv /opt/venv
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir -r requirements.txt
COPY parameter_server ./parameter_server
RUN pip install --no-cache-dir --no-deps . && rm -rf build *.egg-info
COPY config ./config
COPY examples ./examples
USER 10001:10001
EXPOSE 50051
CMD ["python", "-m", "parameter_server", "--config", "config/server.json"]

FROM base AS test
ENV RUFF_CACHE_DIR=/tmp/ruff-cache
USER root
COPY requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY tests ./tests
COPY scripts ./scripts
USER 10001:10001
CMD ["python", "-m", "pytest", "-p", "no:cacheprovider"]

FROM base AS runtime
