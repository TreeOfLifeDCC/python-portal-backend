# Pinned, slim base image
FROM python:3.10.16-slim

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY ./app /code/app

# Run as an unprivileged user. Because non-root cannot bind ports < 1024,
# the service now listens on 8080 (update the reverse proxy / service
# targetPort accordingly).
RUN useradd --create-home --uid 10001 appuser \
    && chown -R appuser:appuser /code
USER appuser

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--root-path", "/api/"]
