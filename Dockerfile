FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY ./yaml/ ./yaml/
COPY ./app/ ./app/

EXPOSE 3275

CMD [ "sh", "-c", "sleep 30 && cd app && python3 main.py" ]
