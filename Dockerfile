FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN if grep -Eq '^[^#[:space:]]' requirements.txt; then pip install --no-cache-dir -r requirements.txt; fi

COPY . .
RUN chmod +x /app/entrypoint.sh
RUN useradd --create-home --shell /usr/sbin/nologin appuser && chown -R appuser:appuser /app

USER appuser

CMD ["/app/entrypoint.sh"]
