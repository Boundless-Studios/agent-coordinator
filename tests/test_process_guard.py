import os
import signal
import subprocess
import sys
import time

import pytest

import agent_coordinator.process_guard as process_guard


def test_status_pipe_failure_tears_down_spawned_child(monkeypatch) -> None:
    class Child:
        pid = 1234
        returncode = None

    child = Child()
    torn_down = []
    monkeypatch.setattr(os, "read", lambda _fd, _size: b"A")
    monkeypatch.setattr(
        os,
        "write",
        lambda _fd, _data: (_ for _ in ()).throw(BrokenPipeError()),
    )
    monkeypatch.setattr(subprocess, "Popen", lambda *_args, **_kwargs: child)
    monkeypatch.setattr(
        process_guard,
        "_terminate_child_group",
        lambda actual, grace: torn_down.append((actual, grace)),
    )

    result = process_guard.main(
        [
            "--parent-fd",
            "3",
            "--status-fd",
            "4",
            "--grace-seconds",
            "0.2",
            "--keepalive-timeout",
            "5",
            "--",
            "command",
        ]
    )

    assert result == 128 + signal.SIGTERM
    assert torn_down == [(child, 0.2)]


@pytest.mark.skipif(os.name != "posix", reason="process groups require POSIX")
def test_keepalive_expiry_stops_child_without_pipe_eof(tmp_path) -> None:
    child_pid_file = tmp_path / "child.pid"
    parent_read_fd, parent_write_fd = os.pipe()
    status_read_fd, status_write_fd = os.pipe()
    guard = subprocess.Popen(
        (
            sys.executable,
            str(process_guard.__file__),
            "--parent-fd",
            str(parent_read_fd),
            "--status-fd",
            str(status_write_fd),
            "--grace-seconds",
            "0.1",
            "--keepalive-timeout",
            "0.2",
            "--",
            sys.executable,
            "-c",
            (
                "import os,time,pathlib;"
                f"pathlib.Path({str(child_pid_file)!r}).write_text(str(os.getpid()));"
                "time.sleep(60)"
            ),
        ),
        pass_fds=(parent_read_fd, status_write_fd),
        start_new_session=True,
    )
    os.close(parent_read_fd)
    os.close(status_write_fd)
    try:
        os.write(parent_write_fd, b"A")
        assert os.read(status_read_fd, 1) == b"S"
        guard.wait(timeout=5)
        child_pid = int(child_pid_file.read_text())
        for _ in range(100):
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.01)
        else:
            pytest.fail("child survived guard keepalive expiry")
    finally:
        os.close(parent_write_fd)
        os.close(status_read_fd)
        if guard.poll() is None:
            os.killpg(guard.pid, signal.SIGKILL)
