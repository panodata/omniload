FROM python:3.14-slim-trixie

LABEL org.opencontainers.image.source="https://github.com/panodata/omniload" \
      org.opencontainers.image.title="omniload (full)" \
      org.opencontainers.image.description="omniload \"full\" image" \
      org.opencontainers.image.licenses="MIT"

# Configure operating system.
ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=linux

# Enable concurrent multi-arch builds.
# https://github.com/docker/buildx/issues/549#issuecomment-1788297892
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT

# Guidelines that have been followed.
# - https://hynek.me/articles/docker-uv/

# Install the `uv` package manager.
# Security-conscious organizations should package/review uv themselves.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Configure build environment.
# - Tell uv to byte-compile packages for faster application startups.
# - Silence uv complaining about not being able to use hard links.
# - Prevent uv from accidentally downloading isolated Python builds.
# - Install packages into the system Python environment.
ENV UV_COMPILE_BYTECODE=true
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_DOWNLOADS=never
ENV UV_SYSTEM_PYTHON=true

# Install prerequisites.

RUN --mount=type=cache,id=apt-cache-${TARGETARCH}${TARGETVARIANT},target=/var/cache/apt \
    --mount=type=cache,id=apt-lists-${TARGETARCH}${TARGETVARIANT},target=/var/lib/apt \
    true \
    && apt-get update \
    && export ACCEPT_EULA='Y' \
    # Install build dependencies
    && apt-get update \
    && apt-get install -y curl gcc gpg libpq-dev build-essential unixodbc-dev g++ apt-transport-https git

RUN \ 
  # Install pyodbc db drivers for MSSQL and PostgreSQL
  curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft-prod.gpg && \
  curl -sSL https://packages.microsoft.com/config/debian/12/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

RUN --mount=type=cache,id=apt-cache-${TARGETARCH}${TARGETVARIANT},target=/var/cache/apt \
    --mount=type=cache,id=apt-lists-${TARGETARCH}${TARGETVARIANT},target=/var/lib/apt \
    true \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 odbc-postgresql \
    # Update odbcinst.ini to make sure full path to driver is listed, and set CommLog to 0. i.e disables any communication logs to be written to files
    && sed 's/Driver=psql/Driver=\/usr\/lib\/x86_64-linux-gnu\/odbc\/psql/;s/CommLog=1/CommLog=0/' /etc/odbcinst.ini > /tmp/temp.ini \
    && mv -f /tmp/temp.ini /etc/odbcinst.ini

# Install application.
COPY . /src
RUN --mount=type=cache,id=uv-${TARGETARCH}${TARGETVARIANT},target=/root/.cache/uv \
    uv pip install '/src[full]'
RUN rm -rf /src

# Ready.
ENTRYPOINT ["omniload"]
