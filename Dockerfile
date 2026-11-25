FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    MIRROR_DB_PATH=/data/mirror.db

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN mkdir -p /data && useradd --create-home --uid 10001 mirror \
    && chown -R mirror:mirror /app /data
USER mirror

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2)"

# A single worker keeps the SQLite deployment simple and predictable.  The
# service is safe with several workers too (WAL + BEGIN IMMEDIATE), but one is
# enough for an operator-driven mirror and avoids cross-process write churn.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
