def write_bson(path, docs):
    """Write BSON documents concatenated into a single file (on-disk mongodump form)."""
    import bson

    with open(path, "wb") as f:
        for doc in docs:
            f.write(bson.encode(doc))
    return path


def write_cbor(path, value):
    """Write a single top-level CBOR value (an array of records, or one record)."""
    import cbor2

    with open(path, "wb") as f:
        f.write(cbor2.dumps(value))
    return path


def write_msgpack(path, rows, **packb_kwargs):
    """Write records as a stream of concatenated MessagePack maps (the on-disk form)."""
    import msgpack

    with open(path, "wb") as f:
        for row in rows:
            f.write(msgpack.packb(row, use_bin_type=True, **packb_kwargs))
    return path


def write_xml(path, text):
    """Write raw XML ``text`` to ``path`` as UTF-8 bytes."""
    with open(path, "wb") as f:
        f.write(text.encode("utf-8") if isinstance(text, str) else text)
    return path


def write_yaml(path, text):
    """Write raw YAML ``text`` to ``path``."""
    with open(path, "w") as f:
        f.write(text)
    return path
