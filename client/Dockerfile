# Client container: the stdlib-only CLI. Files to upload are mounted at /work.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    OBJSTORE_URL=http://controller:8000

COPY objstore_cli.py /app/objstore_cli.py
WORKDIR /work
ENTRYPOINT ["python", "/app/objstore_cli.py"]
CMD ["--help"]
