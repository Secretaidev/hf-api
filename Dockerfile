FROM python:3.11-slim

# System deps (duckdb ke liye zaroori)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Pehle requirements copy karo (Docker cache ke liye)
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Baaki code copy karo
COPY . .

# Railway apna PORT env var deta hai
ENV PORT=8000

EXPOSE 8000

CMD uvicorn app:app --host 0.0.0.0 --port ${PORT} --workers 4