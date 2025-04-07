FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt /app/

RUN pip install uv
RUN uv pip install --no-cache-dir -r requirements.txt --system

COPY . /app/
