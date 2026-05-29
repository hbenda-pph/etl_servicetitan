FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY servicetitan_common.py .
COPY main.py .
# Asegurarse de copiar los archivos de layout que definen los esquemas de las tablas
COPY layout_*.txt ./

CMD ["python", "main.py"]
