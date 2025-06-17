# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set working directory in the container
WORKDIR /app

# Copy your requirements file and install dependencies
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy your entire project into the container
COPY . .

# Set environment variables if needed (optional)
# ENV API_KEY=your_key
# ENV API_SECRET=your_secret

# Run the script
CMD ["python", "./scalp.py"]
