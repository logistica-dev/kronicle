# docker/init/Dockerfile
FROM python:3.12-slim

WORKDIR /opt/kronicle

# Install Python dependencies
COPY docker/init/requirements-test.txt .

RUN python -m venv .venv
ENV PYTHONPATH="/opt/kronicle/src:/opt/kronicle"
ENV PATH="/opt/kronicle/.venv/bin:$PATH"
RUN . .venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements-test.txt

# Copy project code
COPY src src
COPY scripts scripts

# Default command
ENTRYPOINT ["python", "/opt/kronicle/scripts/init/init.py"]
