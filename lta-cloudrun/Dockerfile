# Use official Python image
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy app code and model
COPY . .

# Expose the port Cloud Run expects
EXPOSE 8080

# Run the Flask app (PORT is read from env in app.py)
CMD ["python", "app.py"]
