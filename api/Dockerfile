# api/Dockerfile
# Imagen oficial ligera de Python
FROM python:3.12-slim

# Evitar escritura de bytecode y habilitar flush automático de stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Directorio de trabajo
WORKDIR /app

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código fuente
COPY main.py .

# Exponer el puerto de Cloud Run
EXPOSE 8080

# Iniciar servidor
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
