FROM python:3.12.9-slim

RUN apt-get update && \
    apt-get install -y build-essential \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN rm /app/requirements.txt

COPY src/ .

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app:app", "--reload", "--host", "0.0.0.0", "--port", "8000"]