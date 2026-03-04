FROM python:3.13.2-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --prefix=/install .

FROM python:3.13.2-slim

WORKDIR /app

COPY --from=builder /install /usr/local

COPY . .

RUN find . -name "__pycache__" -type d -exec rm -rf {} +

EXPOSE 8000

CMD ["python", "-m", "src"]