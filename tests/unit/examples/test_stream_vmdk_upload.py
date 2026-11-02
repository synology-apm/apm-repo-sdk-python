"""Tests for the upload side of examples/hypervisor-restore/stream_vmdk.py: ``HttpUploadOutput`` against a local
HTTP server, and ``export_vmdk``'s stream and two-pass modes."""

from __future__ import annotations

import http.server
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from unit.examples.hypervisor_restore_fakes import ExtentContent


@dataclass
class _Received:
    """What the server got, and how it answers."""

    status: int = 201
    reply: bytes = b'{"ok": true}'
    requests: list[tuple[str, str, dict[str, str], bytes]] = field(default_factory=list)


class _Handler(http.server.BaseHTTPRequestHandler):
    """Records each request, its body read by its ``Content-Length`` or decoded from chunks."""

    received: _Received

    def _body(self) -> bytes:
        if self.headers.get("Transfer-Encoding") != "chunked":
            return self.rfile.read(int(self.headers.get("Content-Length", 0)))
        body = b""
        while size := int(self.rfile.readline().strip(), 16):
            body += self.rfile.read(size)
            self.rfile.readline()
        self.rfile.readline()
        return body

    def _answer(self) -> None:
        self.received.requests.append((self.command, self.path, dict(self.headers), self._body()))
        try:
            self.send_response(self.received.status)
            self.send_header("Content-Length", str(len(self.received.reply)))
            self.end_headers()
            self.wfile.write(self.received.reply)
        except OSError:
            pass  # the client hung up first

    do_POST = do_PUT = _answer

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 (the base class names it so)
        pass


@pytest.fixture
def server() -> Iterator[tuple[str, _Received]]:
    received = _Received()
    handler = type("Handler", (_Handler,), {"received": received})
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}", received
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join()


async def _upload(output: Any, *parts: bytes) -> None:
    for part in parts:
        await output.write(part)
    await output.finish()


# -- HttpUploadOutput ------------------------------------------------------------


async def test_upload_without_a_length_is_sent_chunked(ex: ModuleType, server: tuple[str, _Received]) -> None:
    url, received = server
    output = ex.HttpUploadOutput(f"{url}/nfc/disk-0?x=1", headers={"Cookie": "vmware_soap_session=s"})

    await _upload(output, b"KDMV", b"", b"rest")

    ((method, path, headers, body),) = received.requests
    assert (method, path, body) == ("POST", "/nfc/disk-0?x=1", b"KDMVrest")
    assert headers["Transfer-Encoding"] == "chunked" and "Content-Length" not in headers
    assert headers["Cookie"] == "vmware_soap_session=s"
    assert output.response == b'{"ok": true}'


async def test_upload_with_a_length_frames_the_vmdk(ex: ModuleType, server: tuple[str, _Received]) -> None:
    url, received = server
    output = ex.HttpUploadOutput(url, method="PUT", content_length=6, prefix=b"--b\r\n", suffix=b"\r\n--b--")

    await _upload(output, b"abc", b"def")

    ((method, _, headers, body),) = received.requests
    assert (method, body) == ("PUT", b"--b\r\nabcdef\r\n--b--")
    assert headers["Content-Length"] == str(len(body)) and "Transfer-Encoding" not in headers


async def test_upload_of_an_empty_body_still_sends_the_request(ex: ModuleType, server: tuple[str, _Received]) -> None:
    url, received = server

    await ex.HttpUploadOutput(url, content_length=0).finish()

    assert [body for *_, body in received.requests] == [b""]


async def test_upload_fails_on_a_non_2xx_answer(ex: ModuleType, server: tuple[str, _Received]) -> None:
    url, received = server
    received.status, received.reply = 403, b"permission denied"

    with pytest.raises(ex.RestoreError, match="HTTP 403 Forbidden: permission denied"):
        await _upload(ex.HttpUploadOutput(url), b"x")


async def test_upload_fails_when_the_vmdk_is_not_the_announced_length(
    ex: ModuleType, server: tuple[str, _Received]
) -> None:
    url, received = server
    output = ex.HttpUploadOutput(url, content_length=10)

    with pytest.raises(ex.RestoreError, match="came out 3 B, not the 10 B announced"):
        await _upload(output, b"abc")
    await output.abort()

    assert all(len(body) < 10 for *_, body in received.requests)


async def test_abort_before_any_write_does_nothing(ex: ModuleType) -> None:
    await ex.HttpUploadOutput("http://127.0.0.1:9/never").abort()


# -- export_vmdk -----------------------------------------------------------------


def _disk(ex: ModuleType) -> Any:
    content = ExtentContent(1 << 20, {4096: b"data" * 1000})
    return ex.SourceDisk("d", SimpleNamespace(content=content), content.size)


@pytest.mark.parametrize("two_pass", [False, True])
async def test_export_vmdk_uploads_the_vmdk(ex: ModuleType, server: tuple[str, _Received], two_pass: bool) -> None:
    url, received = server
    lengths: list[int | None] = []

    def open_upload(*, content_length: int | None) -> Any:
        lengths.append(content_length)
        return ex.HttpUploadOutput(url, content_length=content_length)

    upload = await ex.export_vmdk(_disk(ex), "disk 1/1", open_upload, two_pass=two_pass, file_name="d.vmdk")

    ((_, _, headers, body),) = received.requests
    assert body.startswith(b"KDMV") and b'SPARSE "d.vmdk"' in body
    assert upload.response == received.reply
    if two_pass:
        assert lengths == [len(body)] and headers["Content-Length"] == str(len(body))
    else:
        assert lengths == [None] and headers["Transfer-Encoding"] == "chunked"


async def test_export_vmdk_two_pass_sizes_before_it_opens_the_upload(
    ex: ModuleType, capsys: pytest.CaptureFixture[str]
) -> None:
    def open_upload(*, content_length: int | None) -> Any:
        raise RuntimeError(f"opened with {content_length}")

    with pytest.raises(RuntimeError, match=r"opened with \d+"):
        await ex.export_vmdk(_disk(ex), "disk 1/1", open_upload, two_pass=True)

    assert "disk 1/1 (sizing): wrote" in capsys.readouterr().err
