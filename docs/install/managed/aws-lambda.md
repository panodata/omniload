# Lambda container image

Run one `omniload` job as an AWS Lambda function packaged as a container image.
The recipe in [`examples/aws-lambda`] loads newline-delimited JSON objects from
one Amazon S3 bucket into Parquet in another, and one AWS `CloudFormation`
template provisions the buckets, the function and its role.

The recipe suits a bounded job that finishes within one invocation. A job that
needs more than 15 minutes, more memory or more temporary disk space than the
limits under [Settings and limits](#settings-and-limits) does not fit it.

## Usage

The steps below run from the recipe directory, `examples/aws-lambda`.

### Lock the dependencies

The image installs a locked dependency set with hashes, so write the lock before
building. `requirements.in` names only `omniload`: edit it to pin the release you
deploy and to add the extras your job needs, then compile the lock for the
Lambda runtime, Python 3.12 on `x86_64`.

```sh
uv pip compile requirements.in --generate-hashes \
    --python-version 3.12 --python-platform x86_64-manylinux_2_34 \
    -o requirements.txt
```

The image accepts source distributions only for the four packages the
`Dockerfile` names in `SDIST_PACKAGES`. Every other package must install from a
wheel. If your extras pull in another package that publishes no wheel, pass the
whole list as a build argument, the four names plus yours:
`--build-arg SDIST_PACKAGES=clickhouse-sqlalchemy,crcmod,dropboxdrivefs,oss2,<package>`.

### Build the image

Lambda runs single-platform images. Build for `linux/amd64` without provenance
attestations, which would turn the result into a manifest list.

```sh
docker buildx build --platform linux/amd64 --provenance=false \
    --load -t omniload-lambda .
```

Push the image to a private Amazon Elastic Container Registry repository in the
account and region you deploy to.

### Deploy the stack

```sh
aws cloudformation deploy \
    --stack-name omniload-job \
    --template-file template.yaml \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides ImageUri=<account>.dkr.ecr.<region>.amazonaws.com/<repository>:<tag>
```

The stack outputs name the function, the source bucket and the destination
bucket.

```sh
aws cloudformation describe-stacks --stack-name omniload-job \
    --query 'Stacks[0].Outputs'
```

### Run the job

Upload the input under `input/` in the source bucket, with the `.jsonl`
extension.

```sh
aws s3 cp records.jsonl s3://<source-bucket>/input/records.jsonl
```

Invoke the function synchronously. A run can take the full 15 minutes, so set
the client read timeout longer than that, and allow the client a single attempt: a
retry would start a second run.

```sh
AWS_MAX_ATTEMPTS=1 aws lambda invoke \
    --function-name <function-name> \
    --cli-read-timeout 960 \
    response.json
```

Check the result before reading the output. A failed run returns a response
with `FunctionError` set, and `response.json` then holds the error type and
message. Output already in the destination bucket does not show that this
invocation succeeded: a run that fails while reading its input leaves the
previous output in place, and a run that fails while writing, or times out, can
leave it missing or partial.

On success, the Parquet output is under `output/records/` in the destination
bucket.

```sh
aws s3 ls s3://<destination-bucket>/output/records/ --recursive
```

### Remove the stack

Empty both buckets first. `CloudFormation` does not delete a bucket that still
holds objects.

```sh
aws s3 rm s3://<source-bucket> --recursive
aws s3 rm s3://<destination-bucket> --recursive
aws cloudformation delete-stack --stack-name omniload-job
```

## Settings and limits

- **URIs.** The template defaults both the source and the destination URI to a
  plain `s3://`, set through the `SourceUri` and `DestinationUri` parameters. An
  `s3://` URI that names no key reads the function role's credentials from the
  environment, through the AWS default credential chain. Amazon S3 access with
  the role is the design of the recipe. The tests described under
  [What is tested](#what-is-tested) do not verify it against AWS.
- **Function.** `MemorySize` is 2048 MB, `EphemeralStorage` is 2048 MB and
  `Timeout` is 900 seconds, the Lambda maximum. Raise memory and ephemeral
  storage in `template.yaml` for larger inputs.
- **One run at a time.** `ReservedConcurrentExecutions` is 1, per function, so
  runs never overlap. Lambda keeps 100 units of an account's concurrency
  unreserved, so making this reservation when the stack is first deployed needs
  the account's unreserved concurrency to be at least 101, or the deployment
  fails.
- **Output replacement.** The handler loads with the fixed `replace` strategy,
  which deletes the previous output before writing the new one. A successful
  rerun over unchanged input therefore produces the same output, and a run that
  fails while writing, or times out, can leave the output missing or partial
  until the next successful run. Keep the input unchanged while a run is in
  progress.
- **Writable storage.** `/tmp` is the only writable location in the function's
  file system. The handler points `HOME`, `TMPDIR`, `DLT_DATA_DIR` and
  `XDG_CACHE_HOME` there and creates those directories once, when Lambda loads
  the handler module. `/tmp` is temporary storage that Lambda does not keep, but
  its contents can survive warm invocations of the same container, so a run can
  find files an earlier run left there. The job's working files must fit
  `EphemeralStorage`.
- **Role permissions.** The role can list the source bucket and read its
  objects. It can list the destination bucket and read, write, and delete its
  objects. Writing elsewhere
  needs a grant of its own.
- **Emulator endpoints.** For an S3-compatible endpoint, such as a local
  emulator, name it in the URI with `?endpoint_url=`. `pyarrow` ignores the
  `AWS_ENDPOINT_URL` environment variable. On a plain `s3://` URI without keys,
  options other than `endpoint_url` and `region` are not checked, so a misspelled
  one, such as `?endpoint=`, is dropped without an error and the emulator
  endpoint is not used.
- **Native libraries.** A connector that needs a native library needs it added
  to the image: the ODBC connectors need `unixODBC` and the driver, the Db2
  connector needs `clidriver`, and the `rsync` source needs the `oc-rsync`
  binary, or `rsync` with the `binary=rsync` URI option.
  Oracle thin mode needs nothing extra. This recipe targets `linux/amd64`.
  Verify native-driver support before porting it to another architecture.
- **Image size.** The image with the base install is over 3 GB. The recipe's CI
  workflow records the size of each image it builds.

## What is tested

A smoke test deploys the template to the `Floci` AWS emulator and runs the job
end to end, with source `s3://?endpoint_url=<emulator>` and a plain `s3://`
destination writing Parquet. It checks the stack's resources and function
settings, a round trip, a warm sequential rerun, that a failure while reading
the input propagates as a function error, and a read-only, non-root run of the
image through the Lambda runtime interface emulator. The workflow
`.github/workflows/aws-lambda.yml` runs it.

Untested: real AWS, including the role's permissions, request signing, the
session token, the region and pulling the image from Amazon Elastic Container
Registry, and the `arm64` architecture.

## Recipe files

```{literalinclude} ../../../examples/aws-lambda/template.yaml
:caption: template.yaml
:language: yaml
```

```{literalinclude} ../../../examples/aws-lambda/handler.py
:caption: handler.py
:language: python
```

```{literalinclude} ../../../examples/aws-lambda/Dockerfile
:caption: Dockerfile
:language: docker
```

```{literalinclude} ../../../examples/aws-lambda/requirements.in
:caption: requirements.in
:language: text
```

## Prior art

The recipe credits the [`Carabas`] subsystem of `LorryStream`, earlier work on
packaging and deploying AWS Lambda functions.

[`Carabas`]: https://github.com/daq-tools/lorrystream
[`examples/aws-lambda`]: https://github.com/panodata/omniload/tree/main/examples/aws-lambda
