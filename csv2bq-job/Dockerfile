FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY servicetitan_common.py .
COPY main.py .
# Ya no se usan archivos layout_*.txt para los esquemas

CMD ["python", "main.py"]
