FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN useradd -u 1000 -m app && mkdir -p /app/data && chown -R app:app /app
COPY --chown=app:app . .
USER app
CMD ["python", "-m", "reddit.main"]
