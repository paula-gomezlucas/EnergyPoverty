FROM python:3.9-slim

WORKDIR /app

COPY src/load_api.py /app/load_api.py
COPY .env /app/.env
COPY scripts/entrypoint_api_loader.sh /app/entrypoint.sh

# Install Python dependencies
RUN pip install --no-cache-dir pandas elasticsearch requests

ENTRYPOINT ["sh", "/app/entrypoint.sh"]