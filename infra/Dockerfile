# Imagen única para los servicios propios del prototipo (procesador y reenviador del borde).
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY edge ./edge
COPY cloud ./cloud
COPY loadtest ./loadtest
ENV PYTHONUNBUFFERED=1
