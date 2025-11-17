###Script Dockerfile:
FROM python:3.11-slim

# Set basic environment variables
ENV PYTHONUNBUFFERED=1

# Argumento para el script a ejecutar (se convierte a ENV para uso en runtime)
ARG SCRIPT_NAME=servicetitan_all_st_to_json.py
ENV SCRIPT_NAME=${SCRIPT_NAME}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["sh", "-c", "python ${SCRIPT_NAME}"]
