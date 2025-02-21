# Use an official Python runtime
FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app/

# Install dependencies
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Set the default command
CMD ["python", "app.py"]
