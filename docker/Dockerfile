FROM python:3.12.7-slim-bookworm
LABEL authors="Magnus Bakken"

RUN python -m venv venv &&  \
    chmod +x venv/bin/activate && \
    ./venv/bin/activate &&  \
    python -m pip install --upgrade pip --no-cache-dir

RUN ./venv/bin/activate &&  \
    python -m pip install chrontext

COPY server.py testdata.ttl ts1.csv ts2.csv /app/
ENV RUST_LOG=info
EXPOSE 50055
ENTRYPOINT ["python", "/app/server.py"]