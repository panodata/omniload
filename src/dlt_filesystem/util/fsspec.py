import re
from typing import Any
from urllib.parse import urlsplit

from fsspec.utils import update_storage_options


def infer_storage_options(
    urlpath: str, inherit_storage_options: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Infer storage options from URL path and merge it with existing storage
    options.

    Parameters
    ----------
    urlpath: str or unicode
        Either local absolute file path or URL (hdfs://namenode:8020/file.csv)
    inherit_storage_options: dict (optional)
        Its contents will get merged with the inferred information from the
        given path

    Returns
    -------
    Storage options dict.

    Examples
    --------
    >>> infer_storage_options('/mnt/datasets/test.csv')  # doctest: +SKIP
    {"protocol": "file", "path", "/mnt/datasets/test.csv"}
    >>> infer_storage_options(
    ...     'hdfs://username:pwd@node:123/mnt/datasets/test.csv?q=1',
    ...     inherit_storage_options={'extra': 'value'},
    ... )  # doctest: +SKIP
    {"protocol": "hdfs", "username": "username", "password": "pwd",
    "host": "node", "port": 123, "path": "/mnt/datasets/test.csv",
    "url_query": "q=1", "extra": "value"}
    """
    # Handle Windows paths including disk name in this special case
    # TODO: A `+` character was added to the second regex.
    #       https://github.com/fsspec/filesystem_spec/pull/2085
    if (
        re.match(r"^[a-zA-Z]:[\\/]", urlpath)
        or re.match(r"^[a-zA-Z0-9+]+://", urlpath) is None
    ):
        return {"protocol": "file", "path": urlpath}

    parsed_path = urlsplit(urlpath)
    protocol = parsed_path.scheme or "file"
    if parsed_path.fragment:
        path = "#".join([parsed_path.path, parsed_path.fragment])
    else:
        path = parsed_path.path
    if protocol == "file":
        # Special case parsing file protocol URL on Windows according to:
        # https://msdn.microsoft.com/en-us/library/jj710207.aspx
        windows_path = re.match(r"^/([a-zA-Z])[:|]([\\/].*)$", path)
        if windows_path:
            drive, path = windows_path.groups()
            path = f"{drive}:{path}"

    if protocol in ["http", "https"]:
        # for HTTP, we don't want to parse, as requests will anyway
        return {"protocol": protocol, "path": urlpath}

    options: dict[str, Any] = {"protocol": protocol, "path": path}

    if parsed_path.netloc:
        # Parse `hostname` from netloc manually because `parsed_path.hostname`
        # lowercases the hostname which is not always desirable (e.g. in S3):
        # https://github.com/dask/dask/issues/1417
        options["host"] = parsed_path.netloc.rsplit("@", 1)[-1].rsplit(":", 1)[0]

        if protocol in ("s3", "s3a", "gcs", "gs"):
            options["path"] = options["host"] + options["path"]
        else:
            options["host"] = options["host"]
        if parsed_path.port:
            options["port"] = parsed_path.port
        if parsed_path.username:
            options["username"] = parsed_path.username
        if parsed_path.password:
            options["password"] = parsed_path.password

    if parsed_path.query:
        options["url_query"] = parsed_path.query
    if parsed_path.fragment:
        options["url_fragment"] = parsed_path.fragment

    if inherit_storage_options:
        update_storage_options(options, inherit_storage_options)

    return options
