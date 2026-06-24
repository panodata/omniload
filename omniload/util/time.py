import datetime
import struct
from typing import Optional


def isotime(dt: Optional[datetime.datetime]) -> Optional[str]:
    """
    Converts a datetime object to an iso 8601 formatted string.
    """
    if dt is None:
        return None
    return dt.isoformat()


def handle_datetimeoffset(dto_value: bytes) -> datetime.datetime:
    # ref: https://github.com/mkleehammer/pyodbc/issues/134#issuecomment-281739794
    tup = struct.unpack(
        "<6hI2h", dto_value
    )  # e.g., (2017, 3, 16, 10, 35, 18, 500000000, -6, 0)
    return datetime.datetime(
        tup[0],
        tup[1],
        tup[2],
        tup[3],
        tup[4],
        tup[5],
        tup[6] // 1000,
        datetime.timezone(datetime.timedelta(hours=tup[7], minutes=tup[8])),
    )
