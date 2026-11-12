# Controller node: metadata, placement, failure detection, repair.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATA_DIR=/data

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY objstore_controller ./objstore_controller

VOLUME ["/data"]
EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/v1/health', timeout=2)"

CMD ["uvicorn", "--factory", "objstore_controller.main:create_app", "--host", "0.0.0.0", "--port", "8000"]
