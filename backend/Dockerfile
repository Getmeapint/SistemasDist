# Usar a imagem base do Python
FROM python:3.9-slim

# Instalar dependências
RUN pip install fastapi uvicorn[standard] aio-pika aiokafka

# Copiar o código do backend
WORKDIR /app
COPY backend.py /app/backend.py

# Expor a porta 8000
EXPOSE 8000

# Comando para iniciar o backend
CMD ["python", "-u", "-m", "uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000"]




# Use a lightweight Python image
# FROM python:3.10-slim

# # Set work directory
# WORKDIR /app

# # Install dependencies
# RUN pip install kafka-python

# # Copy consumer script
# COPY backend.py .

# # Environment variables (can be overridden at runtime)
# ENV KAFKA_BOOTSTRAP_SERVERS=kafka-service:9092
# ENV KAFKA_TOPIC_PATTERN=race-
# ENV KAFKA_RETRIES=10
# ENV KAFKA_RETRY_DELAY=3

# # Run the consumer
# CMD ["python", "-u","backend.py"]



# FROM python:3.10-slim

# WORKDIR /app

# RUN pip install --no-cache-dir fastapi uvicorn[standard] aiokafka

# COPY backend.py .

# EXPOSE 8000

# ENV PYTHONUNBUFFERED=1

# CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]




# FROM python:3.10-slim

# WORKDIR /app

# RUN pip install --no-cache-dir fastapi uvicorn aiokafka kafka-python

# COPY backend.py .

# EXPOSE 8000

# ENV PYTHONUNBUFFERED=1

# CMD ["uvicorn", "backend:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]