FROM python:3.12-slim

WORKDIR /app

# Instalar dbt-bigquery y sus utilidades base (las dependencias apt son para C++ en caso de ser requeridas por dbt, aunque en slim usualmente basta)
RUN apt-get update -y && apt-get install -y git && apt-get clean
RUN pip install --no-cache-dir dbt-bigquery==1.8.3

COPY . .

# Instalar paquetes estipulados en dbt (ej: dbt_utils)
RUN dbt deps --profiles-dir .

# Se usa un full-refresh garantizado o simplemente run, el flag lo pasaremos o lo mantendremos en default run 
CMD ["dbt", "run", "--profiles-dir", "."]
