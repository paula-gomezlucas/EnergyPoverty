FROM python:3.9-slim

WORKDIR /app

COPY src/load_csv_to_es.py /app/load_csv_to_es.py
COPY .env /app/.env
COPY scripts/entrypoint_csv_loader.sh /app/entrypoint.sh

RUN pip install elasticsearch pandas

ENTRYPOINT ["sh", "/app/entrypoint.sh"]