# Base Image: Use a lightweight Python version
FROM python:3.9-slim

# Set environment variables to keep Python clean
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app


COPY requirements.txt .

# Install dependencies (This happens during build, so you need internet HERE, but not later)
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose the port Streamlit runs on
EXPOSE 8501

# Define the command to run the app
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]