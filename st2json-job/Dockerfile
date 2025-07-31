###Script Dockerfile:
FROM python:3.11-slim

# (Opcional) WORKDIR /
# RUN apt-get update && apt-get install -y gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "servicetitan_all_st_to_json.py"]
