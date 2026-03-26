FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY nyc_taxi_injector.py .

# Iniciar la inyección de forma continua a velocidad normal (10 msg/s)
CMD ["python", "nyc_taxi_injector.py", "--limit", "0", "--rate", "10"]
