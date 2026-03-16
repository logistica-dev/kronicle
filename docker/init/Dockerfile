# docker/init/Dockerfile
FROM python:3.12-slim

ENV KRONICLE_HOME=/opt/kronicle
WORKDIR $KRONICLE_HOME

# Install Python dependencies
COPY docker/init/requirements-test.txt .

RUN python -m venv .venv
ENV PYTHONPATH="$KRONICLE_HOME/src:$KRONICLE_HOME"
ENV PATH="$KRONICLE_HOME/.venv/bin:$PATH"
RUN . .venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements-test.txt

# Copy project code
COPY src src
COPY scripts scripts

# Default command
ENTRYPOINT ["python", "$KRONICLE_HOME/scripts/init/init.py"]
