"""Smoke test of the AWS Lambda recipe in ``examples/aws-lambda``.

The recipe's CloudFormation template is deployed to Floci, which runs the
function image in a container of its own, and the job is invoked end to end:
newline-delimited JSON in one bucket, Parquet in the other. A second harness
runs the same image read-only and as a non-root user through the Lambda runtime
interface emulator the base image ships, because Floci does not make the root
file system read-only as Lambda does.

Build the image first, then name it in ``OMNILOAD_TEST_LAMBDA_IMAGE``; without
it the Docker tests skip. The build installs ``requirements.txt``, the hashed
lock, which the checkout does not hold: compile it first, as the Dockerfile's
header says (the CI workflow locks the checkout's own wheel instead)::

    cd examples/aws-lambda
    uv pip compile requirements.in --generate-hashes \\
        --python-version 3.12 --python-platform x86_64-manylinux_2_34 \\
        -o requirements.txt
    cd ../..
    docker buildx build --platform linux/amd64 --provenance=false --load \\
        -t omniload-lambda:smoke examples/aws-lambda
    OMNILOAD_TEST_LAMBDA_IMAGE=omniload-lambda:smoke \\
        pytest tests/warehouse/managed/test_aws_lambda.py -n0 --no-cov

What Floci does not check, and so this test does not either: the role's S3
permissions (Floci evaluates no inline IAM policy), request signatures, and the
session token real Lambda credentials carry.
"""

import io
import json
import os
import secrets
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.util.container.impl.floci import FlociContainer
from tests.warehouse.manager import FLOCI_IMAGE

IMAGE = os.environ.get("OMNILOAD_TEST_LAMBDA_IMAGE", "")
RECIPE = Path(__file__).resolve().parents[3] / "examples" / "aws-lambda"
TEMPLATE = RECIPE / "template.yaml"
REGION = "us-east-1"
FLOCI_PORT = 4566
#: Label on every Docker object this module creates, valued with a per-run token,
#: so teardown removes only what this run started.
RUN_LABEL = "org.omniload.test.lambda-run"

#: The job's input, and what the Parquet output must hold.
RECORDS = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
INPUT_KEY = "input/records.jsonl"
OUTPUT_PREFIX = "output/records/"
READONLY_OUTPUT = "output-readonly/records"

#: Lambda's synchronous invoke can run the full 900 s timeout. A shorter client
#: read timeout, or a client retry, looks like a failure and starts a second run.
CLIENT_CONFIG = {"read_timeout": 960, "retries": {"max_attempts": 0}}

requires_image = pytest.mark.skipif(
    not IMAGE, reason="OMNILOAD_TEST_LAMBDA_IMAGE names no function image"
)


# --- the template, read as CloudFormation ---


def _cloudformation_loader() -> type[yaml.SafeLoader]:
    """A YAML loader that reads CloudFormation's short-form intrinsic tags."""

    class Loader(yaml.SafeLoader):
        pass

    def intrinsic(loader, suffix, node):
        if isinstance(node, yaml.ScalarNode):
            value: Any = loader.construct_scalar(node)
        elif isinstance(node, yaml.SequenceNode):
            value = loader.construct_sequence(node, deep=True)
        else:
            value = loader.construct_mapping(node, deep=True)
        if suffix == "Ref":
            return {"Ref": value}
        if suffix == "GetAtt" and isinstance(value, str):
            value = value.split(".", 1)
        return {f"Fn::{suffix}": value}

    Loader.add_multi_constructor("!", intrinsic)
    return Loader


def load_template() -> dict:
    return yaml.load(TEMPLATE.read_text(), Loader=_cloudformation_loader())  # noqa: S506


def function_properties() -> dict:
    return load_template()["Resources"]["JobFunction"]["Properties"]


def test_stack_template_grants():
    """The role trusts Lambda only, and its inline grants are exactly the actions
    the job needs on the two buckets and their objects. Floci does not evaluate
    inline IAM policies, so this is the only check of them short of a real AWS
    account."""
    role = load_template()["Resources"]["ExecutionRole"]["Properties"]

    (trust,) = role["AssumeRolePolicyDocument"]["Statement"]
    assert trust["Effect"] == "Allow"
    assert trust["Principal"] == {"Service": "lambda.amazonaws.com"}
    assert trust["Action"] == "sts:AssumeRole"

    grants = set()
    for policy in role["Policies"]:
        for statement in policy["PolicyDocument"]["Statement"]:
            assert statement["Effect"] == "Allow"
            actions = statement["Action"]
            for action in [actions] if isinstance(actions, str) else actions:
                for resource in statement["Resource"]:
                    grants.add((action, json.dumps(resource, sort_keys=True)))
    source = {"Fn::GetAtt": ["SourceBucket", "Arn"]}
    source_objects = {"Fn::Sub": "${SourceBucket.Arn}/*"}
    destination = {"Fn::GetAtt": ["DestinationBucket", "Arn"]}
    destination_objects = {"Fn::Sub": "${DestinationBucket.Arn}/*"}
    # The source is read only; `replace` deletes the previous output, so the
    # destination's objects need delete as well as read and write.
    expected = [
        ("s3:ListBucket", source),
        ("s3:GetObject", source_objects),
        ("s3:ListBucket", destination),
        ("s3:DeleteObject", destination_objects),
        ("s3:GetObject", destination_objects),
        ("s3:PutObject", destination_objects),
    ]
    assert grants == {
        (action, json.dumps(resource, sort_keys=True)) for action, resource in expected
    }


def test_stack_template_runs_one_job_at_a_time():
    """`replace` rewrites one output, so two overlapping runs would interleave.
    The limits are also stated on the recipe's documentation page."""
    properties = function_properties()
    assert properties["ReservedConcurrentExecutions"] == 1
    assert properties["Timeout"] == 900
    assert properties["PackageType"] == "Image"
    assert properties["MemorySize"] == 2048
    assert properties["EphemeralStorage"] == {"Size": 2048}


# --- Docker fixtures: everything carries this run's label ---


@dataclass
class Stack:
    name: str
    function: str
    source_bucket: str
    destination_bucket: str
    status: str


@pytest.fixture(scope="module")
def run_token() -> str:
    return secrets.token_hex(4)


@pytest.fixture(scope="module")
def run_labels(run_token) -> dict[str, str]:
    return {RUN_LABEL: run_token}


@pytest.fixture(scope="module")
def docker_client():
    import docker

    client = docker.from_env()
    yield client
    client.close()


def _run_containers(docker_client, run_token: str, name: str | None = None) -> list:
    """This run's containers, found by its label (and an exact name, if given)."""
    filters = {"label": f"{RUN_LABEL}={run_token}"}
    if name is not None:
        filters["name"] = name
    return [
        container
        for container in docker_client.containers.list(all=True, filters=filters)
        if name is None or container.name == name
    ]


def _remove_after_failed_start(docker_client, run_token: str, name: str) -> None:
    """A create that times out on the client can still complete in the daemon,
    leaving a container the caller holds no handle to. Remove it, by this run's
    label and its exact name, so a failed start leaves nothing behind."""
    for container in _run_containers(docker_client, run_token, name):
        container.remove(force=True, v=True)


@pytest.fixture(scope="module")
def lambda_network(docker_client, run_token, run_labels):
    """A network of this run's own, which Floci starts function containers on."""
    network = docker_client.networks.create(
        f"omniload-lambda-{run_token}", labels=run_labels
    )
    yield network
    # Anything of this run's still here (a fixture that failed part way) would
    # keep the network in use.
    for container in _run_containers(docker_client, run_token):
        container.remove(force=True, v=True)
    network.remove()


def _function_containers(docker_client, network_name: str, floci_id: str) -> list:
    """Containers Floci started on this run's network, found by the network."""
    return [
        container
        for container in docker_client.containers.list(
            all=True, filters={"network": network_name}
        )
        if container.id != floci_id and container.name.startswith("floci-")
    ]


def _start_floci(docker_client, network_name: str, run_token: str, run_labels):
    """Start this run's Floci, leaving no container behind if the start fails."""
    name = f"omniload-lambda-{run_token}-floci"
    container = FlociContainer(
        image=FLOCI_IMAGE,
        edge_port=FLOCI_PORT,
        region_name=REGION,
        name=name,
        docker_socket=True,
        lambda_network=network_name,
        labels=run_labels,
    )
    try:
        container.start()
    except BaseException:
        _remove_after_failed_start(docker_client, run_token, name)
        raise
    return container


@pytest.fixture(scope="module")
def floci(docker_client, lambda_network, run_token, run_labels):
    """Floci with the Docker socket, so it can run the function image.

    A run targeting only this directory boots none of the shared database
    containers (tests/conftest.py, covered by tests/main/test_conftest_containers.py);
    a broader run may, and this module neither needs nor objects to them."""
    container = _start_floci(docker_client, lambda_network.name, run_token, run_labels)
    floci_id = container.get_wrapped_container().id
    try:
        yield container
    finally:
        for function_container in _function_containers(
            docker_client, lambda_network.name, floci_id
        ):
            function_container.remove(force=True, v=True)
        # Removes the container together with its anonymous data volume.
        container.stop()


def floci_internal_url(floci) -> str:
    """Floci's address as the function containers on its network see it."""
    return f"http://{floci.get_wrapped_container().name}:{FLOCI_PORT}"


def _wait_for_stack(cloudformation, name: str, timeout: float = 300) -> str:
    """Wait for a stack to settle and return its status; a deleted stack may
    disappear from the listing instead of reporting `DELETE_COMPLETE`."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            stacks = cloudformation.describe_stacks(StackName=name)["Stacks"]
        except cloudformation.exceptions.ClientError as error:
            if "does not exist" in str(error):
                return "DELETE_COMPLETE"
            raise
        status = stacks[0]["StackStatus"]
        if not status.endswith("_IN_PROGRESS") or time.monotonic() > deadline:
            return status
        time.sleep(2)


def _empty_bucket(s3, bucket: str) -> None:
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for item in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=item["Key"])


@pytest.fixture(scope="module")
def stack(floci, run_token) -> Any:
    cloudformation = floci.get_client("cloudformation")
    name = f"omniload-lambda-{run_token}"
    cloudformation.create_stack(
        StackName=name,
        TemplateBody=TEMPLATE.read_text(),
        Capabilities=["CAPABILITY_IAM"],
        Parameters=[
            {"ParameterKey": "ImageUri", "ParameterValue": IMAGE},
            # pyarrow, which reads the source, ignores AWS_ENDPOINT_URL, so the
            # emulator's endpoint goes in the URI. The destination's botocore
            # honours the variable Floci sets, so it stays bare.
            {
                "ParameterKey": "SourceUri",
                "ParameterValue": f"s3://?endpoint_url={floci_internal_url(floci)}",
            },
            {"ParameterKey": "DestinationUri", "ParameterValue": "s3://"},
        ],
    )
    status = _wait_for_stack(cloudformation, name)
    description = cloudformation.describe_stacks(StackName=name)["Stacks"][0]
    outputs = {o["OutputKey"]: o["OutputValue"] for o in description["Outputs"]}
    yield Stack(
        name=name,
        function=outputs["FunctionName"],
        source_bucket=outputs["SourceBucketName"],
        destination_bucket=outputs["DestinationBucketName"],
        status=status,
    )
    # Real AWS refuses to delete a stack whose buckets hold objects.
    s3 = floci.get_client("s3")
    for bucket in (outputs["SourceBucketName"], outputs["DestinationBucketName"]):
        _empty_bucket(s3, bucket)
    cloudformation.delete_stack(StackName=name)
    _wait_for_stack(cloudformation, name)


@pytest.fixture(scope="module")
def s3(floci):
    return floci.get_client("s3")


@pytest.fixture(scope="module")
def lambda_client(floci):
    from botocore.config import Config

    return floci.get_client("lambda", config=Config(**CLIENT_CONFIG))


@pytest.fixture
def job_input(s3, stack):
    """Put the job's input in place before each test, and after it."""
    body = "".join(json.dumps(record) + "\n" for record in RECORDS).encode()

    def put(content: bytes = body) -> None:
        s3.put_object(Bucket=stack.source_bucket, Key=INPUT_KEY, Body=content)

    put()
    yield put
    put()


def invoke(lambda_client, function: str) -> tuple[dict, Any]:
    response = lambda_client.invoke(FunctionName=function, Payload=b"{}")
    return response, json.loads(response["Payload"].read() or b"null")


def output_rows(s3, bucket: str, prefix: str = OUTPUT_PREFIX) -> list[dict]:
    """Every row in the Parquet files under the prefix, without dlt's columns."""
    import pyarrow.parquet as pq

    rows = []
    listing = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for item in listing.get("Contents", []):
        if not item["Key"].endswith(".parquet"):
            continue
        body = s3.get_object(Bucket=bucket, Key=item["Key"])["Body"].read()
        for row in pq.read_table(io.BytesIO(body)).to_pylist():
            rows.append({k: v for k, v in row.items() if not k.startswith("_dlt")})
    return sorted(rows, key=lambda row: row["id"])


def output_objects(s3, bucket: str) -> dict[str, str]:
    listing = s3.list_objects_v2(Bucket=bucket, Prefix="output/")
    return {item["Key"]: item["ETag"] for item in listing.get("Contents", [])}


def serving_containers(docker_client, function: str) -> list[str]:
    return [
        container.id
        for container in docker_client.containers.list(
            filters={"name": f"floci-{function}"}
        )
    ]


# --- G1: the stack and the resources it says it created ---


@requires_image
def test_stack_resources(stack, floci, s3):
    """Floci reports unsupported resource types as created, so each resource is
    looked up in its own service, not only in the stack's bookkeeping."""
    assert stack.status == "CREATE_COMPLETE"
    s3.head_bucket(Bucket=stack.source_bucket)
    s3.head_bucket(Bucket=stack.destination_bucket)

    cloudformation = floci.get_client("cloudformation")
    resources = cloudformation.describe_stack_resources(StackName=stack.name)
    role = next(
        r["PhysicalResourceId"]
        for r in resources["StackResources"]
        if r["LogicalResourceId"] == "ExecutionRole"
    )
    floci.get_client("iam").get_role(RoleName=role.rsplit("/", 1)[-1])

    lambda_client = floci.get_client("lambda")
    configuration = lambda_client.get_function_configuration(
        FunctionName=stack.function
    )
    expected = function_properties()
    assert configuration["PackageType"] == expected["PackageType"]
    assert configuration["MemorySize"] == expected["MemorySize"]
    assert configuration["Timeout"] == expected["Timeout"]
    assert configuration["EphemeralStorage"] == expected["EphemeralStorage"]
    assert configuration["Architectures"] == expected["Architectures"]
    concurrency = lambda_client.get_function_concurrency(FunctionName=stack.function)
    assert (
        concurrency["ReservedConcurrentExecutions"]
        == expected["ReservedConcurrentExecutions"]
    )


# --- G2-G4: the job on Floci ---


@requires_image
def test_roundtrip(stack, s3, lambda_client, job_input):
    response, payload = invoke(lambda_client, stack.function)

    assert "FunctionError" not in response, payload
    assert payload["load_ids"]
    assert output_rows(s3, stack.destination_bucket) == RECORDS


@requires_image
def test_warm_rerun_is_idempotent(stack, s3, lambda_client, docker_client, job_input):
    """A second run over unchanged input, in the same warm container, leaves the
    same output: the handler loads with `replace`."""
    response, payload = invoke(lambda_client, stack.function)
    assert "FunctionError" not in response, payload
    before = serving_containers(docker_client, stack.function)

    response, payload = invoke(lambda_client, stack.function)

    assert "FunctionError" not in response, payload
    assert output_rows(s3, stack.destination_bucket) == RECORDS
    after = serving_containers(docker_client, stack.function)
    assert len(before) == 1
    assert after == before


@requires_image
def test_extract_failure_propagates(stack, s3, lambda_client, job_input):
    """A failing run is reported as a function error and leaves the previous
    output alone, and the next run in the same environment succeeds.

    Floci reports a Python handler exception as `Handled` where AWS reports
    `Unhandled`, so only the presence of the error is asserted.
    """
    response, payload = invoke(lambda_client, stack.function)
    assert "FunctionError" not in response, payload
    previous_output = output_objects(s3, stack.destination_bucket)

    job_input(b'{"id": 1, "name": "alpha"}\nnot json\n')
    response, payload = invoke(lambda_client, stack.function)

    assert "FunctionError" in response
    assert payload["errorType"] == "PipelineStepFailed"
    assert output_objects(s3, stack.destination_bucket) == previous_output

    job_input()
    response, payload = invoke(lambda_client, stack.function)
    assert "FunctionError" not in response, payload
    assert output_rows(s3, stack.destination_bucket) == RECORDS


# --- G5: read-only root file system, non-root user ---


@pytest.fixture
def emulator(docker_client, lambda_network, floci, stack, run_token, run_labels):
    """The image as Lambda runs it: read-only except /tmp, not root.

    The base image starts its runtime interface emulator when no Lambda runtime
    API is present. The credentials are placeholders Floci accepts.
    """
    container = _start_emulator(
        docker_client,
        lambda_network.name,
        run_token,
        run_labels,
        floci_internal_url(floci),
        stack,
    )
    try:
        container.reload()
        yield container
    finally:
        container.remove(force=True, v=True)


def _start_emulator(
    docker_client, network_name: str, run_token: str, run_labels, floci_url, stack
):
    """Start this run's emulator, leaving no container behind if the start fails."""
    name = f"omniload-lambda-{run_token}-rie"
    try:
        return _run_emulator(
            docker_client, name, network_name, run_labels, floci_url, stack
        )
    except BaseException:
        _remove_after_failed_start(docker_client, run_token, name)
        raise


def _run_emulator(docker_client, name, network_name, run_labels, floci_url, stack):
    return docker_client.containers.run(
        IMAGE,
        detach=True,
        name=name,
        labels=run_labels,
        network=network_name,
        read_only=True,
        tmpfs={"/tmp": "rw,size=512m,uid=993,gid=990"},  # noqa: S108
        user="993:990",
        mem_limit="2g",
        ports={"8080/tcp": ("127.0.0.1", None)},
        environment={
            "AWS_ACCESS_KEY_ID": "test",
            "AWS_SECRET_ACCESS_KEY": "test",
            "AWS_SESSION_TOKEN": "test",
            "AWS_REGION": REGION,
            "AWS_ENDPOINT_URL": floci_url,
            "OMNILOAD_SOURCE_URI": f"s3://?endpoint_url={floci_url}",
            "OMNILOAD_SOURCE_TABLE": f"{stack.source_bucket}/input/*.jsonl",
            "OMNILOAD_DEST_URI": "s3://",
            "OMNILOAD_DEST_TABLE": f"{stack.destination_bucket}/{READONLY_OUTPUT}",
        },
    )


def invoke_emulator(container, timeout: float = 960) -> Any:
    port = container.ports["8080/tcp"][0]["HostPort"]
    url = f"http://127.0.0.1:{port}/2015-03-31/functions/function/invocations"
    deadline = time.monotonic() + 30
    while True:
        try:
            request = urllib.request.Request(url, data=b"{}", method="POST")
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read() or b"null")
        except (ConnectionError, urllib.error.URLError):
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


@requires_image
def test_readonly_root(emulator, s3, stack, job_input):
    """The job writes nothing outside /tmp. The emulator answers 200 even when
    the handler fails, so the body is what signals an error."""
    attrs = emulator.attrs
    assert attrs["HostConfig"]["ReadonlyRootfs"] is True
    assert attrs["Config"]["User"] == "993:990"
    probe = emulator.exec_run(["/bin/sh", "-c", "touch /var/task/probe"])
    assert probe.exit_code != 0
    assert b"Read-only file system" in probe.output

    for _ in range(2):
        payload = invoke_emulator(emulator)
        assert "errorType" not in payload, payload
        assert payload["load_ids"]
        assert output_rows(s3, stack.destination_bucket, READONLY_OUTPUT) == RECORDS

    log = emulator.logs().decode(errors="replace")
    assert "Read-only file system" not in log
