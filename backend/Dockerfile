# Usar a imagem base do Python
FROM python:3.9-slim

# Install dependencies (including Prometheus instrumentation)
RUN pip install --no-cache-dir fastapi uvicorn[standard] aio-pika aiohttp prometheus-fastapi-instrumentator

WORKDIR /app
COPY backend.py /app/backend.py

EXPOSE 8000

CMD ["python", "-u", "-m", "uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000"]
