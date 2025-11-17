FROM python:3.11-slim

# (Opcional) WORKDIR /
# RUN apt-get update && apt-get install -y gcc

# Argumento para el script a ejecutar (se convierte a ENV para uso en runtime)
ARG SCRIPT_NAME=servicetitan_all_json_to_bigquery.py
ENV SCRIPT_NAME=${SCRIPT_NAME}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sh", "-c", "python ${SCRIPT_NAME}"]
