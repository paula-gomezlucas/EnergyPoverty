FROM python:3.9-slim

WORKDIR /app

COPY src/load_api.py /app/load_api.py
COPY .env /app/.env
COPY scripts/entrypoint_api_loader.sh /app/entrypoint.sh

RUN pip install elasticsearch requests

ENTRYPOINT ["sh", "/app/entrypoint.sh"]