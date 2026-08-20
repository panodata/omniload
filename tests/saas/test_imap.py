import imaplib
import os
import time
from email.message import Message

import duckdb
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import PortWaitStrategy

from tests.util import invoke_ingest_command

pytestmark = pytest.mark.integration


class DovecotContainer(DockerContainer):
    """
    A Testcontainer for Dovecot IMAP.
    """

    DOVECOT_VERSION = os.environ.get("DOVECOT_VERSION", "2.4.4")

    def __init__(
        self,
        image: str = f"docker.io/dovecot/dovecot:{DOVECOT_VERSION}",
        imap_port: int = 31993,
        **kwargs,
    ) -> None:
        super().__init__(image=image, **kwargs)
        self.imap_port = imap_port
        self.with_env("USER_PASSWORD", "secret")
        # TODO: Because the connector will always connect to port 993,
        #       we need to use port _binding_ here.
        self.with_bind_ports(f"{self.imap_port}/tcp", 993)
        self.waiting_for(PortWaitStrategy(self.imap_port))


@pytest.fixture
def dovecot():
    """Fixture for providing a Dovecot server."""
    container = DovecotContainer()
    container.start()
    # TODO: Get rid of `time.sleep`.
    time.sleep(1)
    try:
        host = container.get_container_host_ip()
        yield host
    finally:
        container.stop()


@pytest.fixture
def dovecot_with_message(dovecot):
    """Fixture for providing a Dovecot server including a single message in `INBOX`."""
    imap = imaplib.IMAP4_SSL(host=dovecot, port=993)
    imap.login("hotzenplotz", "secret")

    new_message = Message()
    new_message["From"] = "hello@example.org"
    new_message["Subject"] = "Test mail."
    new_message["Date"] = "Thu, 20 Aug 2026 11:35:19 +0200"
    new_message.set_payload("This is the message.")

    # TODO: Alternatively use given mailbox name than just `INBOX`.
    # imap.create("testdrive")
    imap.append(
        "INBOX",
        "",
        imaplib.Time2Internaldate(time.time()),
        str(new_message).encode("utf-8"),
    )
    imap.logout()
    yield dovecot


def test_imap_basic(dovecot_with_message, tmp_path):
    """Verify a basic ingest from an IMAP mailbox."""

    abs_db_path = tmp_path / "test_imap.duckdb"
    uri = f"duckdb:///{abs_db_path}"

    result = invoke_ingest_command(
        f"imap://?host={dovecot_with_message}&username=hotzenplotz&password=secret",
        "",
        uri,
        "raw.imap",
    )
    assert result.exit_code == 0, result.output

    conn = duckdb.connect(abs_db_path)
    result = conn.sql("select count(*) from raw.imap").fetchone()
    assert result is not None, "Database result is empty"
    assert result[0] > 0, "No records found in table raw.imap"
    conn.close()
