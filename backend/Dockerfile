# Usar a imagem base do Python
FROM python:3.9-slim

RUN pip install fastapi uvicorn[standard] aio-pika aiokafka

WORKDIR /app
COPY backend.py /app/backend.py

EXPOSE 8000

CMD ["python", "-u", "-m", "uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000"]
