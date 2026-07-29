import pytest

from omniload.source.rsync.config import query_params
from omniload.source.rsync.error import (
    InvalidRsyncUriError,
    UnsupportedTransportError,
)
from omniload.source.rsync.transport import (
    DaemonTransport,
    SshTransport,
    transport_for,
)


def _transport(uri):
    return transport_for(uri, query_params(uri))


# --- factory ---------------------------------------------------------------


def test_factory_dispatches_ssh():
    assert isinstance(_transport("rsync+ssh://user@host/data"), SshTransport)


def test_factory_dispatches_daemon():
    assert isinstance(_transport("rsync://user@host/mod"), DaemonTransport)


def test_factory_rejects_unknown_scheme():
    with pytest.raises(UnsupportedTransportError):
        transport_for("ftp://host/x", {})


# --- SSH transport ---------------------------------------------------------


@pytest.mark.parametrize(
    ("remote_root", "expected"),
    [
        ("/srv/data", "deploy@example.com:/srv/data/"),
        ("/srv/data/", "deploy@example.com:/srv/data/"),
    ],
)
def test_ssh_remote_spec_normalises_trailing_slash(remote_root, expected):
    t = _transport("rsync+ssh://deploy@example.com")
    assert t.remote_spec(remote_root) == expected


@pytest.mark.parametrize(
    ("remote_root", "expected"),
    [
        ("data", "example.com:data/"),
        ("data/", "example.com:data/"),
    ],
)
def test_ssh_remote_spec_without_user(remote_root, expected):
    t = _transport("rsync+ssh://example.com")
    assert t.remote_spec(remote_root) == expected


def test_ssh_default_shell_emits_no_dash_e():
    t = _transport("rsync+ssh://host")
    assert t.command_args() == []


def test_ssh_key_and_port_compose_remote_shell():
    t = _transport("rsync+ssh://host?ssh_key=/keys/id_ed25519&ssh_port=2222")
    args = t.command_args()
    assert args[0] == "-e"
    assert args[1] == "ssh -p 2222 -i /keys/id_ed25519"


def test_ssh_options_are_shell_split_into_the_shell():
    t = _transport("rsync+ssh://host?ssh_options=-o%20StrictHostKeyChecking%3Dno")
    assert t.command_args() == [
        "-e",
        "ssh -o StrictHostKeyChecking=no",
    ]


def test_ssh_rsh_overrides_everything():
    t = _transport("rsync+ssh://host?rsh=my-wrapper%20--flag&ssh_key=/k")
    assert t.command_args() == ["-e", "my-wrapper --flag"]


def test_ssh_namespace_is_secret_free_and_stable():
    t = _transport("rsync+ssh://deploy@Host:2222")
    assert t.storage_namespace() == "rsync+ssh:host:2222:deploy"


def test_ssh_requires_host():
    with pytest.raises(InvalidRsyncUriError):
        transport_for("rsync+ssh:///data", {})


# --- daemon transport ------------------------------------------------------


@pytest.mark.parametrize(
    ("remote_root", "expected"),
    [
        ("mod/exports", "rsync://user@host:873/mod/exports/"),
        ("mod/exports/", "rsync://user@host:873/mod/exports/"),
    ],
)
def test_daemon_remote_spec_normalises_trailing_slash(remote_root, expected):
    t = _transport("rsync://user@host/mod")
    assert t.remote_spec(remote_root) == expected


def test_daemon_custom_port_from_netloc():
    t = _transport("rsync://host:8730/mod")
    assert t.port == 8730
    assert t.remote_spec("mod") == "rsync://host:8730/mod/"


def test_daemon_command_args_include_port_and_no_motd():
    t = _transport("rsync://host/mod")
    assert t.command_args() == ["--port", "873", "--no-motd"]


def test_daemon_no_motd_can_be_disabled():
    t = _transport("rsync://host/mod?no_motd=false")
    assert "--no-motd" not in t.command_args()


def test_daemon_password_file_goes_to_argv_not_env():
    t = _transport("rsync://user@host/mod?password_file=/etc/rsync.pw")
    assert "--password-file" in t.command_args()
    assert t.environment() == {}


def test_daemon_inline_password_goes_to_env_not_argv():
    t = _transport("rsync://user:s3cret@host/mod")
    assert t.environment() == {"RSYNC_PASSWORD": "s3cret"}
    assert "s3cret" not in " ".join(t.command_args())


def test_daemon_password_file_wins_over_inline_password():
    t = _transport("rsync://user:s3cret@host/mod?password_file=/etc/rsync.pw")
    assert t.environment() == {}


def test_daemon_namespace_is_secret_free_and_stable():
    t = _transport("rsync://user:pw@Host:8730/mod")
    assert t.storage_namespace() == "rsync:host:8730:user"


def test_daemon_requires_host():
    with pytest.raises(InvalidRsyncUriError):
        transport_for("rsync:///mod", {})
