FROM python:3.11-slim

WORKDIR /app

# System dependencies for Pillow, numpy, lxml, OpenCV etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgl1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Persistent runtime state (SQLite index, resume progress, captcha stats) lives
# here; docker-compose mounts it as a volume so it survives restarts.
RUN mkdir -p /app/data

# Default process is the 24/7 collector. docker-compose overrides this for the
# on-demand search service.
CMD ["python", "run_all.py"]
