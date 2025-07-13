FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code and data directory
COPY . .

# Run the main script
CMD ["python3", "main.py"]
