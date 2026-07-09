"""Shared test doubles for the iterabledata-backed readers (msgpack, cbor).

These stand in for ``dlt``'s ``FileItemDict`` so a reader can be driven directly, without a
real filesystem source, to exercise chunking and the non-seekable spool.
"""


class FileItemStub:
    """Minimal ``FileItemDict`` stand-in: a reader only calls ``open()`` as a context manager
    yielding a binary stream, so a real ``FileItemDict`` is not needed."""

    def __init__(self, path):
        """Wrap a filesystem ``path`` that ``open()`` will read in binary mode."""
        self._path = path

    def open(self):
        """Open the backing file as a binary stream."""
        return open(self._path, "rb")


class NonSeekableStream:
    """A read-only binary stream that refuses to seek, mimicking a remote/pipe fsspec handle.

    iterabledata's ctor rewinds via ``seek(0)`` and raises on such a handle, so the reader must
    spool it into a BytesIO first.
    """

    def __init__(self, data):
        """Hold ``data`` (bytes) and start the read cursor at the beginning."""
        self._data = data
        self._pos = 0

    def seekable(self):
        """Always ``False``; this stream refuses to seek."""
        return False

    def read(self, n=-1):
        """Read up to ``n`` bytes (all remaining when ``n`` is negative), advancing the cursor."""
        if n is None or n < 0:
            chunk = self._data[self._pos :]
            self._pos = len(self._data)
        else:
            chunk = self._data[self._pos : self._pos + n]
            self._pos += len(chunk)
        return chunk

    def __enter__(self):
        """Enter the context manager, returning this stream unchanged."""
        return self

    def __exit__(self, *exc):
        """Exit the context manager without suppressing exceptions."""
        return False


class NonSeekableItem:
    """A ``FileItemDict`` stand-in whose ``open()`` yields a non-seekable stream, to prove the
    reader spools such a handle before decoding."""

    def __init__(self, data):
        """Hold the ``data`` (bytes) the opened stream will serve."""
        self._data = data

    def open(self):
        """Open the data as a non-seekable binary stream."""
        return NonSeekableStream(self._data)
