FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backfill_deleted_at.py backfill_created_at_from_supabase.py ./
COPY utils ./utils/

# Default: run deleted_at backfill (override with command for created_at)
CMD ["python", "backfill_deleted_at.py"]
