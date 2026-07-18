(oracle-oci)=

# OCI

[Oracle Cloud Infrastructure Object Storage (OCI)] is an internet-scale,
high-performance storage platform that offers reliable and cost-efficient
data durability provided by Oracle. The Object Storage service can store
an unlimited amount of unstructured data of any content type.

`omniload` supports OCI as a data source.

## URI format

The URI for connecting to OCI is structured as follows.
```text
oci://<my_bucket>@<my_namespace>/<my_prefix>/data.parquet?iam_type=api_key&config={"user":"ocid1.user.oc1..24g4uzg","region":"us-ashburn-1","tenancy":"ocid1.tenancy.oc1..23423r3","key_file":"/path/to/key.pem","fingerprint":"06:8c:ce:5b:4a:b5:53:d4:f8:e0:d2:58:63:1c:8d:d2"}
```

## URI parameters

:config:
  Config for the connection to OCI, see [SDK and CLI Configuration File].
  If a dict, it should be returned from `oci.config.from_file`.
  If a str, it should be the location of the config file.
  If None, user should have a "resource principal" configured environment.
  If resource principal is not available, use instance principal.
  Use JSON to encode the dictionary.
  Type: `dict` or `str`. Default: None.

:signer:
  A signer from the OCI sdk. More info: oci.auth.signers
  Type: `oci.auth.signers`. Default: None.

:profile:
  The profile to use from the config, if the config is passed in.

:iam_type:
  The IAM Auth principal type to use.
  Values can be one of "api_key", "resource_principal", "instance_principal",
  "oke_principal", "unknown_signer".

:compartment_id:
  The OCID of the compartment to scope the authorization to. If not
  provided, the tenancy's root compartment will be used.

:region:
  The region identifier that the client should connect to.
  Regions can be found here:
  https://docs.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm

:block_size:
  The block size in bytes. Default: `5242880` (`5MB`).

:config_kwargs:
  Dictionary of parameters passed to the OCI Client upon connection.
  Use JSON to encode the dictionary.
  Type: `dict`. Default: None.

:oci_additional_kwargs:
  Dictionary of parameters that are used when calling OCI api
  methods. Typically used for things like "retry_strategy".
  Use JSON to encode the dictionary.
  Type: `dict`. Default: None.

:more...:
  Other parameters for OCI session.
  This includes default parameters for tenancy, namespace, and region.

## Set up an OCI integration

To integrate `omniload` with Oracle Cloud Infrastructure Object Storage,
you need a storage account and one of the supported credentials. You can
start with the [free tier] and later [upgrade your account].

## Examples

### Load data from OCI

```shell
omniload ingest \
    --source-uri 'oci://bucket@namespace?iam_type=api_key&config={"user":"ocid1.user.oc1..24g4uzg","region":"us-ashburn-1","tenancy":"ocid1.tenancy.oc1..23423r3","key_file":"/path/to/key.pem","fingerprint":"06:8c:ce:5b:4a:b5:53:d4:f8:e0:d2:58:63:1c:8d:d2"}' \
    --source-table '/prefix/data.parquet' \
    --dest-uri 'duckdb:///example.duckdb' \
    --dest-table 'testdrive.data'
```


[free tier]: https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier.htm
[Oracle Cloud Infrastructure Object Storage (OCI)]: https://docs.oracle.com/en-us/iaas/Content/Object/Concepts/objectstorageoverview.htm
[SDK and CLI Configuration File]: https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm
[upgrade your account]: https://docs.oracle.com/en-us/iaas/Content/Billing/Tasks/changingpaymentmethod.htm
