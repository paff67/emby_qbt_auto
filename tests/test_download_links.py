from __future__ import annotations

import base64
import hashlib
import http.server
import socket
import threading
from contextlib import contextmanager
from dataclasses import FrozenInstanceError

import pytest
import qbt_orchestrator.download_links as download_links

from qbt_orchestrator.download_links import (
    HttpMetainfoResolver,
    HttpResponse,
    LinkResolutionError,
    ResolvedDownloadLink,
    parse_download_link,
)
from qbt_orchestrator.download_links import _HttpBodyStream, _safe_response_headers


V1 = "0123456789abcdef0123456789abcdef01234567"
V2 = "23" * 32
MAGNET_V1 = "mag" + "net:?xt=urn:btih:"


def _bc_link(*, name: str = "A.mkv", size: int = 10, infohash: str = V1) -> str:
    plain = f"AA/{name}/{size}/{infohash}/ZZ".encode("utf-8")
    return "bc://bt/" + base64.b64encode(plain).decode("ascii")


def _torrent(info: bytes, *, prefix: bytes = b"") -> bytes:
    return b"d" + prefix + b"4:info" + info + b"e"


def _v1_info(*, name: bytes = b"a", extra: bytes = b"") -> bytes:
    return (
        b"d6:lengthi1e4:name"
        + str(len(name)).encode("ascii")
        + b":"
        + name
        + b"12:piece lengthi16384e6:pieces20:"
        + b"x" * 20
        + extra
        + b"e"
    )


def _v2_tree() -> bytes:
    return (
        b"d1:ad0:d6:lengthi1e11:pieces root32:"
        + b"r" * 32
        + b"eee"
    )


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, url, *, timeout_sec, resolved_addresses, server_hostname):
        self.calls.append(
            (url, timeout_sec, tuple(resolved_addresses), server_hostname)
        )
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _response(status, body=b"", *, headers=None, peer_ip="93.184.216.34"):
    return HttpResponse(
        status=status,
        headers=headers or {},
        body=body,
        peer_ip=peer_ip,
    )


@contextmanager
def _local_metainfo_server(body: bytes, *, protocol: str, connection_close: bool):
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = protocol

        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/x-bittorrent")
            self.send_header("Content-Length", str(len(body)))
            if connection_close:
                self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def public_dns(monkeypatch):
    def resolve(host, port, *, type=0):
        assert type == socket.SOCK_STREAM
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)


def test_result_is_frozen_and_repr_does_not_leak_original_or_metainfo():
    original = MAGNET_V1 + V1 + "&tr=https://secret.invalid/token"
    result = parse_download_link(original)

    with pytest.raises(FrozenInstanceError):
        result.kind = "other"
    shown = repr(result)
    assert original not in shown
    assert "secret.invalid" not in shown
    assert "original=" not in shown


def test_magnet_normalizes_hex_and_base32_btih():
    assert parse_download_link(MAGNET_V1 + "AB" * 20).infohash_v1 == "ab" * 20
    assert (
        parse_download_link(
            MAGNET_V1 + "AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH"
        ).infohash_v1
        == V1
    )


def test_magnet_supports_v2_and_hybrid_without_display_or_tracker_identity():
    value = (
        MAGNET_V1 + V1.upper()
        + "&xt=urn:btmh:1220"
        + V2.upper()
        + "&dn=private-name&tr=https%3A%2F%2Ftracker.invalid%2Ftoken"
    )
    result = parse_download_link(value)
    assert result.infohash_v1 == V1
    assert result.infohash_v2 == V2
    assert result.redacted == (
        MAGNET_V1 + V1 + "&xt=urn:btmh:1220" + V2
    )
    assert "private-name" not in result.redacted
    assert "tracker" not in result.redacted


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        ("magnet:?dn=no-identity", "magnet_missing_identity"),
        (MAGNET_V1 + "xyz", "invalid_magnet_identity"),
        (MAGNET_V1 + V1 + "&xt=urn:btih:xyz", "invalid_magnet_identity"),
        (MAGNET_V1 + V1 + "&xt=urn:btih:" + V1, "magnet_identity_conflict"),
        (MAGNET_V1 + V1 + "&xt=urn:btih:" + "ab" * 20, "magnet_identity_conflict"),
        ("magnet:?xt=urn:btmh:1221" + V2, "invalid_magnet_identity"),
        (MAGNET_V1 + V1 + "%ZZ", "malformed_percent_encoding"),
        (MAGNET_V1 + V1 + "\n", "control_character"),
        ("MAGNET:?xt=urn:btih:" + V1, "unsupported_link_scheme"),
    ],
)
def test_magnet_rejects_malformed_or_conflicting_identity(value, reason):
    with pytest.raises(LinkResolutionError, match=f"^{reason}$") as caught:
        parse_download_link(value)
    assert caught.value.reason == reason


def test_input_length_and_encoding_are_bounded():
    with pytest.raises(LinkResolutionError, match="input_too_long"):
        parse_download_link(MAGNET_V1 + V1 + "&dn=" + "a" * 8192)
    with pytest.raises(LinkResolutionError, match="input_encoding"):
        parse_download_link(MAGNET_V1 + V1 + "\ud800")


def test_bc_link_decodes_official_envelope_and_extracts_identity():
    value = _bc_link(name="A%20movie.mkv", size=10, infohash="AB" * 20)
    parsed = parse_download_link(value)
    assert parsed.kind == "bc_link"
    assert parsed.infohash_v1 == "ab" * 20
    assert parsed.infohash_v2 is None
    assert parsed.redacted == "bc://bt/" + "ab" * 20


def test_bc_link_requires_canonical_base64_padding_bits():
    canonical = _bc_link(name="a", size=1)
    assert canonical.endswith("o=")
    noncanonical = canonical[:-2] + "p="
    assert base64.b64decode(canonical[8:]) == base64.b64decode(noncanonical[8:])
    with pytest.raises(LinkResolutionError, match="^invalid_bc_link$"):
        parse_download_link(noncanonical)


@pytest.mark.parametrize(
    "value",
    [
        "bc://bt/not-base64!",
        "bc://bt/" + base64.b64encode(b"AA/name/10/not-a-hash/ZZ").decode(),
        "bc://bt/" + base64.b64encode(b"AA/name/10/" + V1.encode()).decode(),
        "bc://bt/" + base64.b64encode(b"AA/name/-1/" + V1.encode() + b"/ZZ").decode(),
        "bc://bt/" + base64.b64encode(b"AA/name/01/" + V1.encode() + b"/ZZ").decode(),
    ],
)
def test_bc_link_rejects_invalid_encoding_structure_size_or_identity(value):
    with pytest.raises(LinkResolutionError, match="^invalid_bc_link$"):
        parse_download_link(value)


def test_http_parse_rejects_credentials_fragment_and_unsupported_scheme():
    for value, reason in [
        ("https://user:secret@example.invalid/a.torrent", "url_credentials"),
        ("https://example.invalid/a.torrent#secret", "url_fragment"),
        ("ftp://example.invalid/a.torrent", "unsupported_link_scheme"),
        ("https://[fe80::1%25eth0]/a.torrent", "invalid_url"),
    ]:
        with pytest.raises(LinkResolutionError, match=f"^{reason}$"):
            parse_download_link(value)


def test_response_header_adapter_rejects_ambiguous_security_headers():
    with pytest.raises(LinkResolutionError, match="^invalid_http_headers$"):
        _safe_response_headers([("Content-Length", "10"), ("content-length", "11")])
    with pytest.raises(LinkResolutionError, match="^invalid_http_headers$"):
        _safe_response_headers([("Location", "/a"), ("location", "/b")])
    with pytest.raises(LinkResolutionError, match="^invalid_http_headers$"):
        _safe_response_headers([("Location", "/a"), ("location", "/a")])
    assert _safe_response_headers([("X-Test", "a"), ("X-Test", "b")])["x-test"] == "a, b"


def test_pinned_transport_does_not_retry_ambiguous_received_response(monkeypatch):
    calls = []

    class Sock:
        def __init__(self, address):
            self.address = address

        def getpeername(self):
            return (self.address, 80)

    class Response:
        status = 200

        def getheaders(self):
            return [("Content-Length", "1"), ("content-length", "2")]

        def close(self):
            pass

    class Connection:
        def __init__(self, _host, _port, *, timeout, target_ip):
            calls.append(target_ip)
            self.sock = Sock(target_ip)

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            return Response()

        def close(self):
            pass

    monkeypatch.setattr(download_links, "_PinnedHTTPConnection", Connection)
    with pytest.raises(LinkResolutionError, match="^invalid_http_headers$"):
        download_links.PinnedHttpTransport().request(
            "http://example.invalid/a",
            timeout_sec=15,
            resolved_addresses=("93.184.216.34", "93.184.216.35"),
            server_hostname="example.invalid",
        )
    assert calls == ["93.184.216.34"]


def test_http_result_hashes_exact_info_bytes(public_dns):
    info = _v1_info(name=b"private-title")
    metainfo = _torrent(info, prefix=b"8:announce14:https://x.test")
    transport = FakeTransport([_response(200, [metainfo[:12], metainfo[12:]])])
    url = "https://example.invalid/file.torrent?token=secret"
    result = HttpMetainfoResolver(transport=transport).resolve(url)

    assert result.kind == "https_url"
    assert result.original == url
    assert result.redacted == "https://example.invalid/file.torrent"
    assert result.input_sha256 == hashlib.sha256(url.encode()).hexdigest()
    assert result.infohash_v1 == hashlib.sha1(info).hexdigest()
    assert result.metainfo == metainfo
    assert transport.calls == [
        (url, 15, ("93.184.216.34",), "example.invalid")
    ]


@pytest.mark.parametrize(
    ("protocol", "connection_close"),
    [("HTTP/1.0", False), ("HTTP/1.1", True)],
)
def test_pinned_transport_preserves_peer_for_connection_close_responses(
    protocol, connection_close
):
    info = _v1_info()
    metainfo = _torrent(info)
    with _local_metainfo_server(
        metainfo, protocol=protocol, connection_close=connection_close
    ) as port:
        result = HttpMetainfoResolver(
            allowed_private_hosts={"127.0.0.1"}
        ).resolve(f"http://127.0.0.1:{port}/a.torrent")
    assert result.infohash_v1 == hashlib.sha1(info).hexdigest()


def test_http_resolver_supports_v2_and_hybrid_exact_hashes(public_dns):
    v2_info = (
        b"d9:file tree" + _v2_tree()
        + b"12:meta versioni2e4:name1:a12:piece lengthi16384ee"
    )
    hybrid_info = (
        b"d9:file tree" + _v2_tree()
        + b"6:lengthi1e12:meta versioni2e"
        b"4:name1:a12:piece lengthi16384e6:pieces20:" + b"x" * 20 + b"e"
    )
    resolver = HttpMetainfoResolver(
        transport=FakeTransport([_response(200, _torrent(v2_info)), _response(200, _torrent(hybrid_info))])
    )
    first = resolver.resolve("https://example.invalid/v2.torrent")
    second = resolver.resolve("https://example.invalid/hybrid.torrent")
    assert first.infohash_v1 is None
    assert first.infohash_v2 == hashlib.sha256(v2_info).hexdigest()
    assert second.infohash_v1 == hashlib.sha1(hybrid_info).hexdigest()
    assert second.infohash_v2 == hashlib.sha256(hybrid_info).hexdigest()


@pytest.mark.parametrize(
    "info",
    [
        b"d9:file treed1:ad0:d6:lengthi1eeee12:meta versioni2e4:name1:a12:piece lengthi16384ee",
        b"d9:file tree" + _v2_tree() + b"12:meta versioni2e4:name1:a12:piece lengthi10000ee",
    ],
)
def test_v2_identity_resolution_defers_semantic_validation_to_qbt(public_dns, info):
    result = HttpMetainfoResolver(
        transport=FakeTransport([_response(200, _torrent(info))])
    ).resolve("https://example.invalid/v2")
    assert result.infohash_v1 is None
    assert result.infohash_v2 == hashlib.sha256(info).hexdigest()


def test_v1_identity_resolution_defers_piece_count_validation_to_qbt(public_dns):
    info = _v1_info(extra=b"6:source1:x")
    # Replace the one 20-byte piece hash with two while retaining a one-byte file.
    info = info.replace(b"6:pieces20:" + b"x" * 20, b"6:pieces40:" + b"x" * 40)
    result = HttpMetainfoResolver(
        transport=FakeTransport([_response(200, _torrent(info))])
    ).resolve("https://example.invalid/v1")
    assert result.infohash_v1 == hashlib.sha1(info).hexdigest()


def test_bep47_padding_and_symlink_entries_are_not_rejected_by_resolver(public_dns):
    files = (
        b"l"
        b"d4:attr1:p6:lengthi1ee"
        b"d4:attr1:l4:pathl4:linke12:symlink pathl6:targetee"
        b"e"
    )
    info = (
        b"d5:files" + files
        + b"4:name1:a12:piece lengthi16384e6:pieces20:"
        + b"x" * 20
        + b"e"
    )
    result = HttpMetainfoResolver(
        transport=FakeTransport([_response(200, _torrent(info))])
    ).resolve("https://example.invalid/bep47")
    assert result.infohash_v1 == hashlib.sha1(info).hexdigest()


def test_semantically_suspect_canonical_candidates_are_identity_only(public_dns):
    # length+files is deliberately not called valid here. The resolver only
    # establishes a canonical v1 identity; stopped qBT precheck decides validity.
    v1 = (
        b"d5:filesle6:lengthi1e4:name1:a12:piece lengthi1e6:pieces0:e"
    )
    # A root empty-key file tree is likewise left to qBT/BEP52 validation after
    # exact v2 identity calculation.
    v2_root_file = (
        b"d9:file treed0:d6:lengthi1eee12:meta versioni2e4:name1:a"
        b"12:piece lengthi16384ee"
    )
    # This multi-piece file has no top-level piece-layers dictionary. Identity
    # is still deterministic; the stopped qBT precheck owns acceptance.
    v2_missing_layers = (
        b"d9:file treed1:ad0:d6:lengthi32768e11:pieces root32:"
        + b"r" * 32
        + b"eee12:meta versioni2e4:name1:a12:piece lengthi16384ee"
    )
    resolver = HttpMetainfoResolver(
        transport=FakeTransport(
            [
                _response(200, _torrent(v1)),
                _response(200, _torrent(v2_root_file)),
                _response(200, _torrent(v2_missing_layers)),
            ]
        )
    )
    v1_result = resolver.resolve("https://example.invalid/suspect-v1")
    root_result = resolver.resolve("https://example.invalid/root-file")
    layers_result = resolver.resolve("https://example.invalid/missing-layers")
    assert v1_result.infohash_v1 == hashlib.sha1(v1).hexdigest()
    assert root_result.infohash_v2 == hashlib.sha256(v2_root_file).hexdigest()
    assert layers_result.infohash_v2 == hashlib.sha256(v2_missing_layers).hexdigest()


def test_canonical_non_torrent_info_dictionary_is_rejected(public_dns):
    with pytest.raises(LinkResolutionError, match="^invalid_metainfo$"):
        HttpMetainfoResolver(
            transport=FakeTransport([_response(200, _torrent(b"d4:name1:ae"))])
        ).resolve("https://example.invalid/object")


def test_http_redirect_is_relative_bounded_and_revalidated(public_dns):
    info = _v1_info()
    transport = FakeTransport(
        [
            _response(302, headers={"Location": "/next.torrent"}),
            _response(200, _torrent(info)),
        ]
    )
    result = HttpMetainfoResolver(transport=transport).resolve(
        "https://example.invalid/start"
    )
    assert result.infohash_v1 == hashlib.sha1(info).hexdigest()
    assert transport.calls[1][0] == "https://example.invalid/next.torrent"


def test_http_resolver_rejects_private_initial_and_redirect(monkeypatch):
    def private(host, port, *, type=0):
        address = "169.254.169.254" if host == "metadata.invalid" else "10.0.0.2"
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, port))]

    monkeypatch.setattr(socket, "getaddrinfo", private)
    resolver = HttpMetainfoResolver(transport=FakeTransport([]))
    with pytest.raises(LinkResolutionError, match="^restricted_address$"):
        resolver.resolve("https://metadata.invalid/a.torrent")


def test_http_resolver_rechecks_private_redirect(monkeypatch):
    def mixed(host, port, *, type=0):
        address = "93.184.216.34" if host == "safe.invalid" else "127.0.0.1"
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (address, port))]

    monkeypatch.setattr(socket, "getaddrinfo", mixed)
    transport = FakeTransport(
        [_response(302, headers={"location": "http://private.invalid/torrent"})]
    )
    with pytest.raises(LinkResolutionError, match="^restricted_address$"):
        HttpMetainfoResolver(transport=transport).resolve(
            "https://safe.invalid/a.torrent"
        )


def test_allowed_private_host_is_exact_normalized_name(monkeypatch):
    def private(host, port, *, type=0):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.2", port))]

    monkeypatch.setattr(socket, "getaddrinfo", private)
    info = _v1_info()
    result = HttpMetainfoResolver(
        transport=FakeTransport([_response(200, _torrent(info), peer_ip="10.0.0.2")]),
        allowed_private_hosts={"PRIVATE.INVALID."},
    ).resolve("https://private.invalid/a.torrent")
    assert result.infohash_v1 == hashlib.sha1(info).hexdigest()


def test_any_private_dns_answer_is_rejected(monkeypatch):
    def mixed(host, port, *, type=0):
        return [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", mixed)
    with pytest.raises(LinkResolutionError, match="^restricted_address$"):
        HttpMetainfoResolver(transport=FakeTransport([])).resolve(
            "https://mixed.invalid/a"
        )


@pytest.mark.parametrize("peer", [None, "93.184.216.35", "127.0.0.1"])
def test_transport_must_prove_connected_peer(public_dns, peer):
    transport = FakeTransport([_response(200, _torrent(_v1_info()), peer_ip=peer)])
    with pytest.raises(LinkResolutionError, match="^peer_address_mismatch$"):
        HttpMetainfoResolver(transport=transport).resolve("https://example.invalid/a")


def test_redirect_limits_loops_and_non_http_targets(public_dns):
    redirect = _response(302, headers={"Location": "/again"})
    with pytest.raises(LinkResolutionError, match="^redirect_loop$"):
        HttpMetainfoResolver(transport=FakeTransport([redirect, redirect])).resolve(
            "https://example.invalid/start"
        )
    with pytest.raises(LinkResolutionError, match="^unsupported_redirect_scheme$"):
        HttpMetainfoResolver(
            transport=FakeTransport(
                [_response(302, headers={"Location": "file:///etc/passwd"})]
            )
        ).resolve("https://example.invalid/start")


def test_redirect_count_is_bounded(public_dns):
    responses = [
        _response(302, headers={"Location": f"/{index}"}) for index in range(6)
    ]
    with pytest.raises(LinkResolutionError, match="^too_many_redirects$"):
        HttpMetainfoResolver(transport=FakeTransport(responses)).resolve(
            "https://example.invalid/start"
        )


def test_body_limit_checks_content_length_and_stream(public_dns):
    exact = _torrent(_v1_info())
    resolver = HttpMetainfoResolver(
        transport=FakeTransport(
            [
                _response(200, exact, headers={"Content-Length": str(len(exact))}),
                _response(200, [b"a" * 6, b"b" * 5]),
                _response(200, b"ignored", headers={"content-length": "11"}),
            ]
        ),
        max_bytes=10,
    )
    # Exact configured limit is accepted at transport boundary (then rejected as bencode).
    with pytest.raises(LinkResolutionError, match="^response_too_large$"):
        resolver.resolve("https://example.invalid/exact")
    with pytest.raises(LinkResolutionError, match="^response_too_large$"):
        resolver.resolve("https://example.invalid/stream")
    with pytest.raises(LinkResolutionError, match="^response_too_large$"):
        resolver.resolve("https://example.invalid/header")


def test_response_stream_is_closed_on_redirect_or_early_rejection(public_dns):
    class ClosingBody:
        def __init__(self):
            self.closed = False

        def __iter__(self):
            yield b"ignored"

        def close(self):
            self.closed = True

    redirected = ClosingBody()
    too_large = ClosingBody()
    transport = FakeTransport(
        [
            _response(302, redirected, headers={"Location": "/done"}),
            _response(200, _torrent(_v1_info())),
            _response(200, too_large, headers={"Content-Length": "999"}),
        ]
    )
    resolver = HttpMetainfoResolver(transport=transport, max_bytes=200)
    resolver.resolve("https://example.invalid/start")
    assert redirected.closed is True
    with pytest.raises(LinkResolutionError, match="response_too_large"):
        resolver.resolve("https://example.invalid/large")
    assert too_large.closed is True


def test_pinned_transport_body_closes_connection_even_before_iteration():
    class Response:
        def __init__(self):
            self.closed = False

        def read(self, _size):
            return b""

        def close(self):
            self.closed = True

    class Connection:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    response = Response()
    connection = Connection()
    stream = _HttpBodyStream(response, connection)
    stream.close()
    assert response.closed is True
    assert connection.closed is True


def test_exact_body_limit_is_allowed(public_dns):
    data = _torrent(_v1_info())
    result = HttpMetainfoResolver(
        transport=FakeTransport(
            [_response(200, [data[:3], data[3:]], headers={"Content-Length": str(len(data))})]
        ),
        max_bytes=len(data),
    ).resolve("https://example.invalid/exact")
    assert result.metainfo == data


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"not bencode", "invalid_metainfo"),
        (b"d3:foo3:bare", "metainfo_missing_info"),
        (_torrent(b"1:x"), "metainfo_info_not_dictionary"),
        (_torrent(_v1_info()) + b"junk", "invalid_metainfo"),
        (_torrent(b"d4:name1:a4:name1:bee"), "invalid_metainfo"),
        (_torrent(b"d4:name1:b4:name1:aee"), "invalid_metainfo"),
    ],
)
def test_metainfo_rejects_malformed_missing_non_dict_duplicate_unsorted_or_trailing(
    public_dns, body, reason
):
    with pytest.raises(LinkResolutionError, match=f"^{reason}$"):
        HttpMetainfoResolver(transport=FakeTransport([_response(200, body)])).resolve(
            "https://example.invalid/a"
        )


def test_metainfo_parser_bounds_depth_and_elements(public_dns):
    deep_info = b"l" * 70 + b"e" * 70
    with pytest.raises(LinkResolutionError, match="^metainfo_resource_limit$"):
        HttpMetainfoResolver(
            transport=FakeTransport([_response(200, _torrent(deep_info))])
        ).resolve("https://example.invalid/deep")

    too_many_elements = b"l" + b"0:" * 100_001 + b"e"
    with pytest.raises(LinkResolutionError, match="^metainfo_resource_limit$"):
        HttpMetainfoResolver(
            transport=FakeTransport([_response(200, _torrent(too_many_elements))])
        ).resolve("https://example.invalid/many")

    oversized_string = _torrent(b"d4:name10485761:")
    with pytest.raises(LinkResolutionError, match="^metainfo_resource_limit$"):
        HttpMetainfoResolver(
            transport=FakeTransport([_response(200, oversized_string)])
        ).resolve("https://example.invalid/string")


def test_transport_timeout_and_dns_failure_have_safe_reason_only(monkeypatch, public_dns):
    secret = "https://example.invalid/a?token=very-secret"
    with pytest.raises(LinkResolutionError, match="^http_timeout$") as timeout:
        HttpMetainfoResolver(transport=FakeTransport([TimeoutError(secret)])).resolve(secret)
    assert "secret" not in str(timeout.value)

    def broken(*args, **kwargs):
        raise socket.gaierror("secret.internal")

    monkeypatch.setattr(socket, "getaddrinfo", broken)
    with pytest.raises(LinkResolutionError, match="^dns_resolution_failed$") as dns:
        HttpMetainfoResolver(transport=FakeTransport([])).resolve(secret)
    assert "secret" not in str(dns.value)
