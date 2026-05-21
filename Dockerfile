# Base python image
FROM python:3.10-slim

# Working directory inside the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project
COPY . .

# Run the application
CMD ["python", "start.py"]