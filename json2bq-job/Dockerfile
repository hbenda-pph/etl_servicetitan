FROM python:3.11-slim

# (Opcional) WORKDIR /
# RUN apt-get update && apt-get install -y gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# El modo de ejecución (all / inbox / test) se controla con la variable
# de entorno ETL_MODE, que inyectan los scripts build_deploy.sh al desplegar.
CMD ["python", "main.py"]
