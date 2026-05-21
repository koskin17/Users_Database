# Base python image
FROM python:3.10-slim

# Working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    qtbase5-dev \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project
COPY . .

# Run the application
CMD ["python", "start.py"]