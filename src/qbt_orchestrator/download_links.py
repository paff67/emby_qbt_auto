"""Strict, secret-safe parsing for download links accepted by the Telegram queue."""

from __future__ import annotations

import base64
import binascii
import concurrent.futures
import heapq
import hashlib
import http.client
import ipaddress
import math
import re
import socket
import threading
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Iterable, Mapping, Protocol
from urllib.parse import unquote_to_bytes, urljoin, urlsplit, urlunsplit


_MAX_INPUT_BYTES = 8 * 1024
_DEFAULT_MAX_BODY = 10 * 1024 * 1024
_MAX_BENCODE_DEPTH = 64
_MAX_BENCODE_ELEMENTS = 100_000
_MAX_INTEGER_DIGITS = 80
_DNS_WORKERS = 4
_DNS_PENDING = 8
_HEX = frozenset("0123456789abcdefABCDEF")
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_BTIH_HEX_RE = re.compile(r"[0-9a-fA-F]{40}\Z")
_BTIH_BASE32_RE = re.compile(r"[A-Z2-7a-z]{32}\Z")
_BTMH_SHA256_RE = re.compile(r"1220([0-9a-fA-F]{64})\Z")
_BC_SIZE_RE = re.compile(r"(?:0|[1-9][0-9]{0,19})\Z")
_NUMERIC_HOST_RE = re.compile(r"(?:0[xX][0-9a-fA-F]+|[0-9.]+)\Z")
_DNS_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=_DNS_WORKERS, thread_name_prefix="qbt-dns"
)
_DNS_SLOTS = threading.BoundedSemaphore(_DNS_PENDING)


class LinkResolutionError(ValueError):
    """A stable failure reason that never includes attacker-controlled input."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True)
class ResolvedDownloadLink:
    kind: str
    original: str = field(repr=False)
    redacted: str
    input_sha256: str
    infohash_v1: str | None
    infohash_v2: str | None
    metainfo: bytes | None = field(default=None, repr=False)


@dataclass(frozen=True)
class HttpResponse:
    """Transport result; ``peer_ip`` proves which validated address was used."""

    status: int
    headers: Mapping[str, str] = field(repr=False)
    body: bytes | Iterable[bytes] = field(repr=False)
    peer_ip: str | None


class HttpTransport(Protocol):
    def request(
        self,
        url: str,
        *,
        timeout_sec: float,
        resolved_addresses: tuple[str, ...],
        server_hostname: str,
    ) -> HttpResponse: ...


class _PinnedConnectionMixin:
    def __init__(self, *args, target_ip: str, **kwargs):
        super().__init__(*args, **kwargs)
        self._target_ip = target_ip
        self._create_connection = self._connect_verified_address

    def _connect_verified_address(self, address, timeout=None, source_address=None):
        _host, port = address
        return socket.create_connection(
            (self._target_ip, port), timeout, source_address=source_address
        )

    def close(self):
        # http.client closes self.sock inside getresponse() as soon as headers
        # declare a non-persistent response. Preserve it until the bounded body
        # stream owns and closes the response; otherwise its timeout cannot be
        # tightened against the absolute deadline.
        if getattr(self, "_preserve_socket_for_body", False):
            return
        super().close()


class _PinnedHTTPConnection(_PinnedConnectionMixin, http.client.HTTPConnection):
    pass


class _PinnedHTTPSConnection(_PinnedConnectionMixin, http.client.HTTPSConnection):
    pass


def _safe_response_headers(pairs: Iterable[tuple[str, str]]) -> dict[str, str]:
    grouped: dict[str, list[str]] = {}
    for raw_key, raw_value in pairs:
        key = str(raw_key).lower()
        value = str(raw_value).strip()
        grouped.setdefault(key, []).append(value)
    for sensitive in ("content-length", "location"):
        values = grouped.get(sensitive, [])
        if len(values) > 1:
            _fail("invalid_http_headers")
    return {key: ", ".join(values) for key, values in grouped.items()}


def _remaining_seconds(deadline: float, clock) -> float:
    remaining = deadline - clock()
    if remaining <= 0:
        raise TimeoutError("deadline")
    return remaining


def _quiet_close(value) -> None:
    close = getattr(value, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


class _DeadlineWatchdog:
    """One daemon thread aborts sockets at absolute wall-clock deadlines."""

    def __init__(self):
        self._condition = threading.Condition()
        self._heap: list[tuple[float, int, object]] = []
        self._live: set[int] = set()
        self._next_token = 0
        self._thread: threading.Thread | None = None

    def register(self, delay_sec: float, callback) -> int:
        with self._condition:
            self._next_token += 1
            token = self._next_token
            self._live.add(token)
            heapq.heappush(
                self._heap,
                (time.monotonic() + max(0.0, delay_sec), token, callback),
            )
            if self._thread is None:
                self._thread = threading.Thread(
                    target=self._run,
                    name="qbt-http-deadlines",
                    daemon=True,
                )
                self._thread.start()
            self._condition.notify()
            return token

    def cancel(self, token: int | None) -> None:
        if token is None:
            return
        with self._condition:
            self._live.discard(token)
            self._condition.notify()

    def _run(self) -> None:
        while True:
            callback = None
            with self._condition:
                while callback is None:
                    while self._heap and self._heap[0][1] not in self._live:
                        heapq.heappop(self._heap)
                    if not self._heap:
                        self._condition.wait()
                        continue
                    deadline, token, pending = self._heap[0]
                    delay = deadline - time.monotonic()
                    if delay > 0:
                        self._condition.wait(delay)
                        continue
                    heapq.heappop(self._heap)
                    if token in self._live:
                        self._live.remove(token)
                        callback = pending
            try:
                callback()
            except Exception:
                pass


_HTTP_WATCHDOG = _DeadlineWatchdog()


def _abort_connection(connection, fired: threading.Event) -> None:
    fired.set()
    sock = getattr(connection, "sock", None)
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    _quiet_close(sock)


class _HttpBodyStream:
    def __init__(
        self,
        response,
        connection,
        *,
        socket_ref=None,
        deadline: float | None = None,
        clock=time.monotonic,
        watchdog_token: int | None = None,
        deadline_fired: threading.Event | None = None,
    ):
        self._response = response
        self._connection = connection
        self._socket_ref = socket_ref
        self._deadline = deadline
        self._clock = clock
        self._watchdog_token = watchdog_token
        self._deadline_fired = deadline_fired
        self._closed = False

    def __iter__(self):
        try:
            while not self._closed:
                if self._deadline_fired is not None and self._deadline_fired.is_set():
                    raise TimeoutError("deadline")
                if self._deadline is not None:
                    remaining = _remaining_seconds(self._deadline, self._clock)
                    if self._socket_ref is not None:
                        self._socket_ref.settimeout(remaining)
                read_once = getattr(self._response, "read1", None)
                try:
                    chunk = (
                        read_once(64 * 1024)
                        if callable(read_once)
                        else self._response.read(64 * 1024)
                    )
                except Exception:
                    if (
                        self._deadline_fired is not None
                        and self._deadline_fired.is_set()
                    ):
                        raise TimeoutError("deadline") from None
                    raise
                if not chunk:
                    return
                yield chunk
        finally:
            self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        _HTTP_WATCHDOG.cancel(self._watchdog_token)
        try:
            self._response.close()
        finally:
            self._connection.close()


class PinnedHttpTransport:
    """Minimal production transport whose TCP connection is pinned to checked DNS."""

    def __init__(self, *, clock=time.monotonic):
        self._clock = clock

    def request(
        self,
        url: str,
        *,
        timeout_sec: float,
        resolved_addresses: tuple[str, ...],
        server_hostname: str,
        deadline_monotonic: float | None = None,
    ) -> HttpResponse:
        deadline = (
            deadline_monotonic
            if deadline_monotonic is not None
            else self._clock() + timeout_sec
        )
        parts = urlsplit(url)
        port = parts.port or (443 if parts.scheme == "https" else 80)
        path = urlunsplit(("", "", parts.path or "/", parts.query, ""))
        last_error: Exception | None = None
        for target_ip in resolved_addresses:
            remaining = _remaining_seconds(deadline, self._clock)
            connection_cls = (
                _PinnedHTTPSConnection
                if parts.scheme == "https"
                else _PinnedHTTPConnection
            )
            connection = connection_cls(
                server_hostname,
                port,
                timeout=remaining,
                target_ip=target_ip,
            )
            deadline_fired = threading.Event()
            watchdog_token = _HTTP_WATCHDOG.register(
                remaining,
                lambda connection=connection, fired=deadline_fired: _abort_connection(
                    connection, fired
                ),
            )
            response = None
            try:
                connection.request("GET", path, headers={"Accept": "application/x-bittorrent"})
                if deadline_fired.is_set():
                    _abort_connection(connection, deadline_fired)
                    raise TimeoutError("deadline")
                # getresponse() detaches self.sock for HTTP/1.0 and explicit
                # Connection: close responses. Capture the connected peer while
                # the pinned socket is still owned by the connection.
                peer_ip = (
                    ipaddress.ip_address(connection.sock.getpeername()[0])
                    .compressed.lower()
                    if connection.sock
                    else None
                )
                connected_socket = connection.sock
                if connected_socket is not None:
                    connected_socket.settimeout(
                        _remaining_seconds(deadline, self._clock)
                    )
                connection._preserve_socket_for_body = True
                try:
                    response = connection.getresponse()
                finally:
                    connection._preserve_socket_for_body = False
                headers = _safe_response_headers(response.getheaders())

                response_socket = getattr(
                    getattr(getattr(response, "fp", None), "raw", None),
                    "_sock",
                    None,
                )

                return HttpResponse(
                    status=response.status,
                    headers=headers,
                    body=_HttpBodyStream(
                        response,
                        connection,
                        socket_ref=response_socket or connected_socket,
                        deadline=deadline,
                        clock=self._clock,
                        watchdog_token=watchdog_token,
                        deadline_fired=deadline_fired,
                    ),
                    peer_ip=peer_ip,
                )
            except LinkResolutionError:
                _HTTP_WATCHDOG.cancel(watchdog_token)
                if response is not None:
                    _quiet_close(response)
                _quiet_close(connection)
                raise
            except Exception as exc:
                _HTTP_WATCHDOG.cancel(watchdog_token)
                if response is not None:
                    _quiet_close(response)
                _quiet_close(connection)
                last_error = (
                    TimeoutError("deadline")
                    if deadline_fired.is_set()
                    or self._clock() >= deadline
                    or isinstance(exc, (TimeoutError, socket.timeout))
                    else exc
                )
        if last_error is not None:
            raise last_error
        raise OSError("no address")


def _fail(reason: str):
    raise LinkResolutionError(reason)


def _validated_text(value: str) -> tuple[str, str]:
    if not isinstance(value, str):
        _fail("input_type")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError:
        _fail("input_encoding")
    if not value:
        _fail("empty_input")
    if len(encoded) > _MAX_INPUT_BYTES:
        _fail("input_too_long")
    if _CONTROL_RE.search(value):
        _fail("control_character")
    return value, hashlib.sha256(encoded).hexdigest()


def _validate_percent_encoding(value: str) -> None:
    index = 0
    while True:
        index = value.find("%", index)
        if index < 0:
            return
        if index + 2 >= len(value) or value[index + 1] not in _HEX or value[index + 2] not in _HEX:
            _fail("malformed_percent_encoding")
        index += 3


def _decode_query_component(value: str) -> str:
    _validate_percent_encoding(value)
    try:
        return unquote_to_bytes(value.replace("+", " ")).decode("utf-8", "strict")
    except UnicodeError:
        _fail("malformed_percent_encoding")


def _query_pairs(query: str) -> list[tuple[str, str]]:
    if not query:
        return []
    pairs: list[tuple[str, str]] = []
    for item in query.split("&"):
        if "=" not in item:
            _fail("malformed_magnet_query")
        key, value = item.split("=", 1)
        pairs.append((_decode_query_component(key), _decode_query_component(value)))
    return pairs


def _normalize_btih(value: str) -> str | None:
    if _BTIH_HEX_RE.fullmatch(value):
        return value.lower()
    if _BTIH_BASE32_RE.fullmatch(value):
        try:
            decoded = base64.b32decode(value.upper(), casefold=False)
        except binascii.Error:
            return None
        return decoded.hex() if len(decoded) == 20 else None
    return None


def _parse_magnet(value: str, input_sha256: str) -> ResolvedDownloadLink:
    if not value.startswith("magnet:?"):
        _fail("unsupported_link_scheme")
    try:
        parts = urlsplit(value)
    except ValueError:
        _fail("invalid_magnet")
    if parts.scheme != "magnet" or parts.netloc or parts.path or parts.fragment:
        _fail("invalid_magnet")

    v1_values: list[str] = []
    v2_values: list[str] = []
    for key, topic in _query_pairs(parts.query):
        if key.lower() != "xt":
            continue
        lowered = topic.lower()
        if lowered.startswith("urn:btih:"):
            normalized = _normalize_btih(topic[len("urn:btih:") :])
            if normalized is None:
                _fail("invalid_magnet_identity")
            v1_values.append(normalized)
        elif lowered.startswith("urn:btmh:"):
            match = _BTMH_SHA256_RE.fullmatch(topic[len("urn:btmh:") :])
            if not match:
                _fail("invalid_magnet_identity")
            v2_values.append(match.group(1).lower())
    if len(v1_values) > 1 or len(v2_values) > 1:
        _fail("magnet_identity_conflict")
    if not v1_values and not v2_values:
        _fail("magnet_missing_identity")
    v1 = v1_values[0] if v1_values else None
    v2 = v2_values[0] if v2_values else None
    identities = []
    if v1:
        identities.append("xt=urn:btih:" + v1)
    if v2:
        identities.append("xt=urn:btmh:1220" + v2)
    return ResolvedDownloadLink(
        kind="magnet",
        original=value,
        redacted="magnet:?" + "&".join(identities),
        input_sha256=input_sha256,
        infohash_v1=v1,
        infohash_v2=v2,
    )


def _strict_unquote_name(value: str) -> str:
    _validate_percent_encoding(value)
    try:
        decoded = unquote_to_bytes(value).decode("utf-8", "strict")
    except UnicodeError:
        _fail("invalid_bc_link")
    decoded = unicodedata.normalize("NFKC", decoded)
    if (
        not decoded
        or _CONTROL_RE.search(decoded)
        or "/" in decoded
        or "\\" in decoded
        or decoded in {".", ".."}
    ):
        _fail("invalid_bc_link")
    return decoded


def _parse_bc(value: str, input_sha256: str) -> ResolvedDownloadLink:
    if not value.startswith("bc://bt/"):
        _fail("unsupported_link_scheme")
    encoded = value[len("bc://bt/") :]
    if not encoded or len(encoded) % 4:
        _fail("invalid_bc_link")
    try:
        raw = base64.b64decode(encoded.encode("ascii"), validate=True)
        if base64.b64encode(raw).decode("ascii") != encoded:
            _fail("invalid_bc_link")
        plain = raw.decode("utf-8", "strict")
    except (UnicodeError, ValueError, binascii.Error):
        _fail("invalid_bc_link")
    pieces = plain.split("/")
    if len(pieces) != 5 or pieces[0] != "AA" or pieces[4] != "ZZ":
        _fail("invalid_bc_link")
    _strict_unquote_name(pieces[1])
    if not _BC_SIZE_RE.fullmatch(pieces[2]):
        _fail("invalid_bc_link")
    infohash = _normalize_btih(pieces[3])
    if infohash is None or len(pieces[3]) != 40:
        _fail("invalid_bc_link")
    return ResolvedDownloadLink(
        kind="bc_link",
        original=value,
        redacted="bc://bt/" + infohash,
        input_sha256=input_sha256,
        infohash_v1=infohash,
        infohash_v2=None,
    )


def _normalize_hostname(hostname: str) -> str:
    if not hostname or "%" in hostname or "\\" in hostname:
        _fail("invalid_url")
    hostname = hostname.rstrip(".")
    if not hostname:
        _fail("invalid_url")
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        if _NUMERIC_HOST_RE.fullmatch(hostname):
            _fail("invalid_url")
        try:
            normalized = hostname.encode("idna").decode("ascii").lower()
        except UnicodeError:
            _fail("invalid_url")
        if not normalized or any(not label for label in normalized.split(".")):
            _fail("invalid_url")
        return normalized
    return address.compressed.lower()


def _parse_http_url(value: str) -> tuple[str, str, int, str]:
    if not (value.startswith("http://") or value.startswith("https://")):
        _fail("unsupported_link_scheme")
    _validate_percent_encoding(value)
    try:
        parts = urlsplit(value)
        port = parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError:
        _fail("invalid_url")
    if parts.scheme not in {"http", "https"} or not parts.netloc or parts.hostname is None:
        _fail("invalid_url")
    if parts.username is not None or parts.password is not None:
        _fail("url_credentials")
    if parts.fragment:
        _fail("url_fragment")
    if not (1 <= port <= 65535):
        _fail("invalid_url")
    hostname = _normalize_hostname(parts.hostname)
    redacted_netloc = hostname
    if ":" in hostname:
        redacted_netloc = "[" + hostname + "]"
    if parts.port is not None:
        redacted_netloc += ":" + str(parts.port)
    redacted = urlunsplit((parts.scheme, redacted_netloc, "/…", "", ""))
    return parts.scheme, hostname, port, redacted


def parse_download_link(value: str) -> ResolvedDownloadLink:
    value, input_sha256 = _validated_text(value)
    if value.startswith("magnet:") or value.lower().startswith("magnet:"):
        return _parse_magnet(value, input_sha256)
    if value.startswith("bc://") or value.lower().startswith("bc://"):
        return _parse_bc(value, input_sha256)
    if value.startswith("http://") or value.startswith("https://"):
        scheme, _hostname, _port, redacted = _parse_http_url(value)
        return ResolvedDownloadLink(
            kind=scheme + "_url",
            original=value,
            redacted=redacted,
            input_sha256=input_sha256,
            infohash_v1=None,
            infohash_v2=None,
        )
    _fail("unsupported_link_scheme")


class _BencodeParser:
    def __init__(self, data: bytes):
        self.data = data
        self.position = 0
        self.elements = 0
        self.info_span: tuple[int, int] | None = None

    def _element(self) -> None:
        self.elements += 1
        if self.elements > _MAX_BENCODE_ELEMENTS:
            _fail("metainfo_resource_limit")

    def parse(self):
        if not self.data or self.data[0:1] != b"d":
            _fail("invalid_metainfo")
        value = self._dictionary(0, top_level=True)
        if self.position != len(self.data):
            _fail("invalid_metainfo")
        return value

    def _value(self, depth: int):
        if depth > _MAX_BENCODE_DEPTH:
            _fail("metainfo_resource_limit")
        if self.position >= len(self.data):
            _fail("invalid_metainfo")
        self._element()
        marker = self.data[self.position : self.position + 1]
        if marker == b"i":
            return self._integer()
        if marker == b"l":
            return self._list(depth)
        if marker == b"d":
            return self._dictionary(depth)
        if b"0" <= marker <= b"9":
            return self._bytes()
        _fail("invalid_metainfo")

    def _integer(self) -> int:
        self.position += 1
        end = self.data.find(b"e", self.position)
        if end < 0:
            _fail("invalid_metainfo")
        raw = self.data[self.position : end]
        if not raw or len(raw) > _MAX_INTEGER_DIGITS:
            _fail("invalid_metainfo")
        negative = raw.startswith(b"-")
        digits = raw[1:] if negative else raw
        if not digits or not digits.isdigit():
            _fail("invalid_metainfo")
        if (len(digits) > 1 and digits.startswith(b"0")) or (negative and digits == b"0"):
            _fail("invalid_metainfo")
        self.position = end + 1
        return int(raw)

    def _bytes(self) -> bytes:
        colon = self.data.find(b":", self.position)
        if colon < 0:
            _fail("invalid_metainfo")
        raw_length = self.data[self.position : colon]
        if (
            not raw_length
            or not raw_length.isdigit()
            or (len(raw_length) > 1 and raw_length.startswith(b"0"))
            or len(raw_length) > 20
        ):
            _fail("invalid_metainfo")
        length = int(raw_length)
        if length > _DEFAULT_MAX_BODY:
            _fail("metainfo_resource_limit")
        start = colon + 1
        end = start + length
        if end > len(self.data):
            _fail("invalid_metainfo")
        self.position = end
        return self.data[start:end]

    def _list(self, depth: int) -> list:
        self.position += 1
        result = []
        while True:
            if self.position >= len(self.data):
                _fail("invalid_metainfo")
            if self.data[self.position : self.position + 1] == b"e":
                self.position += 1
                return result
            result.append(self._value(depth + 1))

    def _dictionary(self, depth: int, *, top_level: bool = False) -> dict[bytes, object]:
        self.position += 1
        result: dict[bytes, object] = {}
        previous: bytes | None = None
        while True:
            if self.position >= len(self.data):
                _fail("invalid_metainfo")
            if self.data[self.position : self.position + 1] == b"e":
                self.position += 1
                return result
            self._element()
            if not (b"0" <= self.data[self.position : self.position + 1] <= b"9"):
                _fail("invalid_metainfo")
            key = self._bytes()
            if previous is not None and key <= previous:
                _fail("invalid_metainfo")
            previous = key
            start = self.position
            value = self._value(depth + 1)
            if top_level and key == b"info":
                if self.info_span is not None:
                    _fail("invalid_metainfo")
                self.info_span = (start, self.position)
            result[key] = value


def _metainfo_identities(data: bytes) -> tuple[str | None, str | None]:
    parser = _BencodeParser(data)
    top = parser.parse()
    if b"info" not in top or parser.info_span is None:
        _fail("metainfo_missing_info")
    info = top[b"info"]
    if not isinstance(info, dict):
        _fail("metainfo_info_not_dictionary")
    start, end = parser.info_span
    raw_info = data[start:end]

    # This resolver owns canonical parsing, resource bounds, and identity
    # hashing. It deliberately does not duplicate libtorrent's complete
    # BEP3/BEP47/BEP52 semantic validator: canonical candidates proceed to the
    # stopped qBT precheck, which is the authoritative acceptance boundary.
    is_v2 = type(info.get(b"meta version")) is int and info[b"meta version"] == 2
    is_v1 = (
        isinstance(info.get(b"name"), bytes)
        and type(info.get(b"piece length")) is int
        and isinstance(info.get(b"pieces"), bytes)
        and (b"length" in info or b"files" in info)
    )
    if not is_v1 and not is_v2:
        _fail("invalid_metainfo")
    return (
        hashlib.sha1(raw_info).hexdigest() if is_v1 else None,
        hashlib.sha256(raw_info).hexdigest() if is_v2 else None,
    )


def _is_restricted(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if address.is_multicast or address.is_unspecified:
        return True
    if isinstance(address, ipaddress.IPv6Address):
        if address.is_site_local:
            return True
        if address.ipv4_mapped is not None and not address.ipv4_mapped.is_global:
            return True
    return not address.is_global


def _header(headers: Mapping[str, str], name: str) -> str | None:
    lowered = name.lower()
    values = [str(value).strip() for key, value in headers.items() if str(key).lower() == lowered]
    if not values:
        return None
    if len(set(values)) != 1:
        _fail("invalid_http_headers")
    return values[0]


def _read_body(
    body: bytes | Iterable[bytes],
    max_bytes: int,
    *,
    deadline: float,
    clock,
) -> bytes:
    chunks = [body] if isinstance(body, bytes) else body
    output = bytearray()
    try:
        iterator = iter(chunks)
        while True:
            _remaining_seconds(deadline, clock)
            try:
                chunk = next(iterator)
            except StopIteration:
                _remaining_seconds(deadline, clock)
                break
            _remaining_seconds(deadline, clock)
            if not isinstance(chunk, (bytes, bytearray, memoryview)):
                _fail("http_transport_error")
            if len(output) + len(chunk) > max_bytes:
                _fail("response_too_large")
            output.extend(chunk)
    except LinkResolutionError:
        raise
    except TimeoutError:
        _fail("http_timeout")
    except Exception:
        _fail("http_transport_error")
    return bytes(output)


def _close_body(body: object) -> None:
    _quiet_close(body)


class HttpMetainfoResolver:
    def __init__(
        self,
        *,
        transport: HttpTransport | None = None,
        timeout_sec: float = 15,
        max_bytes: int = _DEFAULT_MAX_BODY,
        max_redirects: int = 5,
        allowed_private_hosts: Iterable[str] | None = None,
        max_resolved_addresses: int = 8,
        clock=time.monotonic,
    ):
        if (
            not isinstance(timeout_sec, (int, float))
            or isinstance(timeout_sec, bool)
            or not math.isfinite(timeout_sec)
            or timeout_sec <= 0
            or not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or not 0 < max_bytes <= _DEFAULT_MAX_BODY
            or not isinstance(max_redirects, int)
            or isinstance(max_redirects, bool)
            or max_redirects < 0
            or not isinstance(max_resolved_addresses, int)
            or isinstance(max_resolved_addresses, bool)
            or not 1 <= max_resolved_addresses <= 32
        ):
            raise ValueError("invalid_http_resolver_limits")
        self.clock = clock
        self.transport = transport or PinnedHttpTransport(clock=clock)
        self.timeout_sec = timeout_sec
        self.max_bytes = max_bytes
        self.max_redirects = max_redirects
        self.max_resolved_addresses = max_resolved_addresses
        self.allowed_private_hosts = frozenset(
            _normalize_hostname(str(host)) for host in (allowed_private_hosts or ())
        )

    def _addresses(
        self, hostname: str, port: int, *, deadline: float
    ) -> tuple[str, ...]:
        try:
            remaining = _remaining_seconds(deadline, self.clock)
        except TimeoutError:
            _fail("resolution_timeout")
        if not _DNS_SLOTS.acquire(timeout=remaining):
            _fail("resolution_timeout")
        try:
            future = _DNS_EXECUTOR.submit(
                socket.getaddrinfo,
                hostname,
                port,
                type=socket.SOCK_STREAM,
            )
        except Exception:
            _DNS_SLOTS.release()
            _fail("dns_resolution_failed")
        future.add_done_callback(lambda _future: _DNS_SLOTS.release())
        try:
            remaining = _remaining_seconds(deadline, self.clock)
            records = future.result(timeout=remaining)
        except (TimeoutError, concurrent.futures.TimeoutError):
            future.cancel()
            _fail("resolution_timeout")
        except Exception:
            _fail("dns_resolution_failed")
        addresses: list[str] = []
        allow_private = hostname in self.allowed_private_hosts
        for record in records:
            try:
                _remaining_seconds(deadline, self.clock)
            except TimeoutError:
                _fail("resolution_timeout")
            try:
                parsed = ipaddress.ip_address(record[4][0])
            except (ValueError, IndexError, TypeError):
                _fail("dns_resolution_failed")
            normalized = parsed.compressed.lower()
            if parsed.is_multicast or parsed.is_unspecified:
                _fail("restricted_address")
            if _is_restricted(parsed) and not allow_private:
                _fail("restricted_address")
            if normalized not in addresses:
                addresses.append(normalized)
                if len(addresses) > self.max_resolved_addresses:
                    _fail("too_many_addresses")
        if not addresses:
            _fail("dns_resolution_failed")
        return tuple(addresses)

    def resolve(self, url: str) -> ResolvedDownloadLink:
        deadline = self.clock() + self.timeout_sec
        unresolved = parse_download_link(url)
        if unresolved.kind not in {"http_url", "https_url"}:
            _fail("unsupported_link_scheme")
        current = url
        visited = {current}
        redirects = 0
        while True:
            scheme, hostname, port, _redacted = _parse_http_url(current)
            addresses = self._addresses(hostname, port, deadline=deadline)
            try:
                remaining = _remaining_seconds(deadline, self.clock)
                request_kwargs = {
                    "timeout_sec": remaining,
                    "resolved_addresses": addresses,
                    "server_hostname": hostname,
                }
                if isinstance(self.transport, PinnedHttpTransport):
                    request_kwargs["deadline_monotonic"] = deadline
                response = self.transport.request(current, **request_kwargs)
            except LinkResolutionError:
                raise
            except (TimeoutError, socket.timeout):
                _fail("http_timeout")
            except Exception:
                _fail("http_transport_error")
            try:
                try:
                    _remaining_seconds(deadline, self.clock)
                except TimeoutError:
                    _fail("http_timeout")
                try:
                    peer = ipaddress.ip_address(response.peer_ip).compressed.lower() if response.peer_ip else None
                except ValueError:
                    peer = None
                if peer is None or peer not in addresses:
                    _fail("peer_address_mismatch")

                if 300 <= response.status <= 399:
                    location = _header(response.headers, "location")
                    if not location:
                        _fail("redirect_missing_location")
                    if redirects >= self.max_redirects:
                        _fail("too_many_redirects")
                    next_url = urljoin(current, location)
                    if not (next_url.startswith("http://") or next_url.startswith("https://")):
                        _fail("unsupported_redirect_scheme")
                    _parse_http_url(next_url)
                    if next_url in visited:
                        _fail("redirect_loop")
                    visited.add(next_url)
                    redirects += 1
                    current = next_url
                    continue
                if not 200 <= response.status <= 299:
                    _fail("http_status")

                content_length = _header(response.headers, "content-length")
                if content_length is not None:
                    if not content_length.isdigit():
                        _fail("invalid_http_headers")
                    if int(content_length) > self.max_bytes:
                        _fail("response_too_large")
                body = _read_body(
                    response.body,
                    self.max_bytes,
                    deadline=deadline,
                    clock=self.clock,
                )
                try:
                    _remaining_seconds(deadline, self.clock)
                except TimeoutError:
                    _fail("http_timeout")
                v1, v2 = _metainfo_identities(body)
                return ResolvedDownloadLink(
                    kind=unresolved.kind,
                    original=url,
                    redacted=unresolved.redacted,
                    input_sha256=unresolved.input_sha256,
                    infohash_v1=v1,
                    infohash_v2=v2,
                    metainfo=body,
                )
            finally:
                _close_body(response.body)


__all__ = [
    "HttpMetainfoResolver",
    "HttpResponse",
    "LinkResolutionError",
    "PinnedHttpTransport",
    "ResolvedDownloadLink",
    "parse_download_link",
]
