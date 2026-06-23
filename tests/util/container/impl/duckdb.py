import shutil
import tempfile
from pathlib import Path

from tests.util.container.model import AbstractService


class EphemeralDuckDb(AbstractService):
    def __init__(self):
        super().__init__()
        self.tmpdir = Path(tempfile.mkdtemp())

    def start(self) -> str:
        abs_path = self.tmpdir / "duckdb.db"
        return f"duckdb:///{abs_path}"

    def start_fully(self) -> str:  # type: ignore
        pass

    def stop(self):
        pass

    def stop_fully(self):
        shutil.rmtree(self.tmpdir)
