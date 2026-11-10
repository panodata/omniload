# check=skip=FromPlatformFlagConstDisallowed
# omniload as an AWS Lambda container image.
#
# Build from this directory, after writing `requirements.txt` with hashes:
#
#   uv pip compile requirements.in --generate-hashes \
#     --python-version 3.12 --python-platform x86_64-manylinux_2_34 \
#     -o requirements.txt
#   docker buildx build --platform linux/amd64 --provenance=false \
#     --load -t omniload-lambda .
#
# Lambda pulls single-platform images, so build for linux/amd64 without
# provenance attestations, which would make the result a manifest list.

# AWS Lambda Python 3.12 base image (Amazon Linux 2023), pinned by digest. The
# platform is fixed because the function is configured for x86_64 only.
FROM --platform=linux/amd64 public.ecr.aws/lambda/python:3.12@sha256:542771a596236be302beae1cb56f5ebd89608b3878b97428bf55b36aaeefa2c2

# Packages in the lock that publish no wheel, and are therefore built from
# source. Every other package must install from a wheel.
ARG SDIST_PACKAGES=clickhouse-sqlalchemy,crcmod,dropboxdrivefs,oss2

# Install the locked dependency set into the function's code directory. The
# build context is mounted rather than copied, so the lock (and any local wheel
# it refers to) stays out of the image.
RUN --mount=type=bind,target=/tmp/build \
    pip install --no-cache-dir --require-hashes \
      --only-binary=:all: --no-binary="${SDIST_PACKAGES}" \
      --requirement /tmp/build/requirements.txt \
      --target "${LAMBDA_TASK_ROOT}"

COPY handler.py ${LAMBDA_TASK_ROOT}/

CMD ["handler.handler"]
