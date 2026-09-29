FROM python:3.12-slim
WORKDIR /workspace
COPY pyproject.toml ./
COPY uv.lock ./
COPY app ./app
RUN pip install --no-cache-dir uv==0.11.14 && uv sync --frozen --no-dev
RUN mkdir -p /workspace/data
CMD [".venv/bin/uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
