FROM python:3.9-slim

WORKDIR /app

COPY src/load_csv.py /app/load_csv.py
COPY .env /app/.env
COPY scripts/entrypoint_csv_loader.sh /app/entrypoint.sh

COPY data/ /app/data/
# COPY data/autoconsumo /app/autoconsumo/
# COPY data/tarifa /app/tarifa/

RUN pip install elasticsearch pandas

ENTRYPOINT ["sh", "/app/entrypoint.sh"]