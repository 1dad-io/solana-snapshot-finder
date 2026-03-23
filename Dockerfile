FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        wget \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system snapshotfinder \
    && useradd --system --create-home --gid snapshotfinder snapshotfinder \
    && mkdir -p /app /snapshots \
    && chown -R snapshotfinder:snapshotfinder /app /snapshots

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --chown=snapshotfinder:snapshotfinder snapshot-finder.py ./snapshot-finder.py
COPY --chown=snapshotfinder:snapshotfinder README.md ./README.md

USER snapshotfinder

VOLUME ["/snapshots"]

ENTRYPOINT ["python", "snapshot-finder.py"]
CMD ["--snapshots", "/snapshots"]
