FROM tiangolo/uvicorn-gunicorn-fastapi:python3.11-slim

WORKDIR /app

# Copy the .env file first
COPY .env ./

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application
COPY ./app ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]