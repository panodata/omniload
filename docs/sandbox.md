# Development sandbox

Acquire sources and install package in development mode.
```shell
git clone https://github.com/panodata/omniload
cd omniload
uv venv --python 3.13 --seed .venv
uv pip install --upgrade --editable='.[full,develop,test]'
```

Run linters and software tests.
```shell
poe check
```

Build local OCI image.
```shell
export BUILDKIT_PROGRESS=plain
docker build --tag=local/omniload --file=release/oci/Dockerfile .
```

Invoke local OCI image.
```shell
docker run --rm -it local/omniload version
```
