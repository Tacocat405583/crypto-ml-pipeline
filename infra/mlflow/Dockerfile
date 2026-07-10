FROM python:3.10.16-slim-bookworm
WORKDIR /app

COPY requirements.txt .
RUN pip install -r /app/requirements.txt

COPY . .

EXPOSE 5500