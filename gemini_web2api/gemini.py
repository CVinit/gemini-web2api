"""Gemini StreamGenerate protocol implementation with httpx streaming."""
import http.client
import json
import secrets
import socket
import string
import threading
import time
import uuid
import re
import urllib.request
import urllib.parse
import ssl
import os
import hashlib
import random
from urllib.error import HTTPError

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False

try:
    import socks  # PySocks, only needed for socks5:// proxies on the urllib path
    HAS_PYSOCKS = True
except ImportError:
    HAS_PYSOCKS = False

from .config import CONFIG


class UpstreamRejected(RuntimeError):
    """Gemini upstream rejected the request; retrying the same payload is pointless."""


_ssl_ctx = None
_ssl_lock = threading.Lock()
_cookie_cache = {"str": "", "sapisid": None, "mtime": 0}
_httpx_client = None
_httpx_lock = threading.Lock()
_socks_opener = None
_socks_opener_lock = threading.Lock()
_http_proxy_opener = None
_http_proxy_opener_key = None
_http_proxy_opener_lock = threading.Lock()


def log(msg: str):
    if CONFIG["log_requests"]:
        import sys
        sys.stderr.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")
        sys.stderr.flush()


def _get_ssl_ctx():
    global _ssl_ctx
    if _ssl_ctx is None:
        with _ssl_lock:
            if _ssl_ctx is None:
                _ssl_ctx = ssl.create_default_context()
    return _ssl_ctx


def _is_socks_proxy(proxy: str) -> bool:
    return bool(proxy) and proxy.lower().startswith(("socks4://", "socks5://", "socks5h://"))


_RANDOM_TOKEN = "{random}"


def _random_label(length: int = 12) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _resolve_proxy(proxy: str) -> str:
    """Expand the {random} placeholder in a proxy URL per request.

    e.g. socks5h://homenet.{random}:<token>@resin.555576.xyz becomes a unique
    username on every call, giving each request its own routing session.
    """
    if not proxy or _RANDOM_TOKEN not in proxy:
        return proxy
    return proxy.replace(_RANDOM_TOKEN, _random_label())


def _retryable(error: Exception) -> bool:
    if isinstance(error, UpstreamRejected):
        return False
    response = getattr(error, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(error, HTTPError):
        status = error.code
    return not (status is not None and 400 <= status < 500 and status != 429)


def _retry_delay(attempt: int) -> float:
    base = max(0.0, float(CONFIG.get("retry_delay_sec", 2)))
    return min(30.0, base * (2 ** attempt)) * random.uniform(0.8, 1.2)




def _parse_socks_proxy(proxy: str) -> tuple:
    """Parse socks5://[user:pass@]host:port into (proxy_type, host, port, user, pass, rdns)."""
    parsed = urllib.parse.urlparse(proxy)
    scheme = parsed.scheme.lower()
    proxy_type = socks.PROXY_TYPE_SOCKS4 if scheme == "socks4" else socks.PROXY_TYPE_SOCKS5
    return proxy_type, parsed.hostname, parsed.port or 1080, parsed.username, parsed.password, scheme == "socks5h"


def _socks_dial(proxy_type, proxy_host, proxy_port, user, password, rdns, dest, timeout):
    """Dial `dest` through a SOCKS proxy, trying IPv4 then IPv6.

    PySocks resolves the proxy hostname within the socket's address family:
    AF_INET fails with getaddrinfo errno -9 on IPv6-only proxy hosts, and
    AF_INET6 fails with errno 97 on hosts without kernel IPv6 support.
    Trying both families covers dual-stack, IPv4-only, and IPv6-only setups.
    """
    last_err = None
    for family in (socket.AF_INET, socket.AF_INET6):
        try:
            sock = socks.socksocket(family, socket.SOCK_STREAM)
        except OSError as e:  # family not supported by this host
            last_err = e
            continue
        sock.set_proxy(proxy_type, proxy_host, proxy_port, rdns=rdns, username=user, password=password)
        if timeout is not None:
            sock.settimeout(timeout)
        try:
            sock.connect(dest)
            return sock
        except OSError as e:
            last_err = e
            sock.close()
    raise last_err


def _get_socks_opener():
    """Build (once) a urllib opener that tunnels HTTPS through a SOCKS proxy.

    Proxy credentials are resolved inside connect() per connection, so a
    {random} placeholder in the proxy URL rotates per request.
    """
    global _socks_opener
    if _socks_opener is None:
        with _socks_opener_lock:
            if _socks_opener is None:
                if not HAS_PYSOCKS:
                    raise RuntimeError("SOCKS proxy requires PySocks: pip install pysocks")

                class SocksHTTPSConnection(http.client.HTTPSConnection):
                    def connect(self):
                        proxy_type, host, port, user, password, rdns = _parse_socks_proxy(
                            _resolve_proxy(CONFIG.get("proxy")))
                        timeout = None if self.timeout is socket._GLOBAL_DEFAULT_TIMEOUT else self.timeout
                        sock = _socks_dial(
                            proxy_type, host, port, user, password, rdns,
                            (self.host, self.port), timeout)
                        # Must wrap in TLS ourselves: overriding connect() bypasses
                        # HTTPSConnection's built-in wrap_socket step.
                        self.sock = self._context.wrap_socket(sock, server_hostname=self.host)

                class SocksHTTPSHandler(urllib.request.HTTPSHandler):
                    def https_open(self, req):
                        return self.do_open(SocksHTTPSConnection, req, context=self._context)

                # Empty ProxyHandler: ignore env HTTP_PROXY/HTTPS_PROXY, all traffic goes via SOCKS.
                _socks_opener = urllib.request.build_opener(
                    urllib.request.ProxyHandler({}), SocksHTTPSHandler(context=_get_ssl_ctx())
                )
    return _socks_opener



def _httpx_limits():
    max_inflight = max(1, int(CONFIG.get("max_inflight_requests", 64)))
    return httpx.Limits(
        max_connections=max(32, max_inflight * 2),
        max_keepalive_connections=max(8, max_inflight // 2),
        keepalive_expiry=30.0,
    )


def _get_httpx_client():
    """Return a shared httpx client, or per-request ones if the proxy URL
    contains a {random} placeholder (credentials must rotate per request)."""
    if HAS_HTTPX and _RANDOM_TOKEN in (CONFIG.get("proxy") or ""):
        proxy = _resolve_proxy(CONFIG.get("proxy"))
        # Unique exit IP per request: keepalive cannot be shared across proxies.
        return httpx.Client(
            proxy=proxy,
            timeout=CONFIG["request_timeout_sec"],
            verify=True,
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
        )
    global _httpx_client
    if _httpx_client is None and HAS_HTTPX:
        with _httpx_lock:
            if _httpx_client is None:
                proxy = CONFIG.get("proxy")
                transport = httpx.HTTPTransport(proxy=proxy, limits=_httpx_limits()) if proxy else httpx.HTTPTransport(limits=_httpx_limits())
                _httpx_client = httpx.Client(
                    transport=transport,
                    timeout=CONFIG["request_timeout_sec"],
                    verify=True,
                    limits=_httpx_limits(),
                )
    return _httpx_client


def _get_http_proxy_opener(proxy: str):
    """Reuse a urllib opener for a stable (non-{random}) HTTP proxy."""
    global _http_proxy_opener, _http_proxy_opener_key
    key = proxy
    if _http_proxy_opener is not None and _http_proxy_opener_key == key:
        return _http_proxy_opener
    with _http_proxy_opener_lock:
        if _http_proxy_opener is None or _http_proxy_opener_key != key:
            _http_proxy_opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": proxy, "https": proxy}),
                urllib.request.HTTPSHandler(context=_get_ssl_ctx())
            )
            _http_proxy_opener_key = key
    return _http_proxy_opener


def load_cookie() -> tuple:
    """Load cookie from file with mtime-based caching."""
    cookie_file = CONFIG.get("cookie_file")
    if not cookie_file or not os.path.exists(cookie_file):
        return "", None
    # Cookie file is hot-reloaded on mtime change; concurrent requests may race
    # on the read, but dict reads of str values are atomic enough in CPython and
    # a torn read at worst returns the previous cookie for one request.
    try:
        mtime = os.path.getmtime(cookie_file)
        if mtime == _cookie_cache["mtime"] and _cookie_cache["str"]:
            return _cookie_cache["str"], _cookie_cache["sapisid"]
        with open(cookie_file, "r") as f:
            content = f.read().strip()
        if content.startswith("{"):
            data = json.loads(content)
            cookie_str = data.get("cookie", "")
            sapisid = data.get("sapisid", "")
        else:
            cookie_str = content
            pairs = dict(p.split("=", 1) for p in cookie_str.split("; ") if "=" in p)
            sapisid = pairs.get("SAPISID", "")
        _cookie_cache.update({"str": cookie_str, "sapisid": sapisid or None, "mtime": mtime})
        return cookie_str, sapisid if sapisid else None
    except Exception as e:
        log(f"Cookie load error: {e}")
        return _cookie_cache["str"], _cookie_cache["sapisid"]


def make_sapisidhash(sapisid: str) -> str:
    ts = int(time.time())
    h = hashlib.sha1(f"{ts} {sapisid} https://gemini.google.com".encode()).hexdigest()
    return f"SAPISIDHASH {ts}_{h}"


def _account_prefix() -> str:
    """Return the Gemini account path prefix for non-default Google accounts."""
    auth_user = CONFIG.get("auth_user")
    if auth_user is None or auth_user == "":
        return ""
    return f"/u/{auth_user}"


def _build_headers() -> dict:
    account_prefix = _account_prefix()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Origin": "https://gemini.google.com",
        "Referer": f"https://gemini.google.com{account_prefix}/app",
        "X-Same-Domain": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    if account_prefix:
        headers["X-Goog-AuthUser"] = str(CONFIG["auth_user"])
    cookie_str, sapisid = load_cookie()
    if cookie_str:
        headers["Cookie"] = cookie_str
    if sapisid:
        headers["Authorization"] = make_sapisidhash(sapisid)
    return headers


def _apply_chat_persistence_flags(inner: list) -> None:
    """Apply Gemini Web persistence flags to an outgoing request payload."""
    if CONFIG.get("temporary_chats", False):
        # Match Gemini Web temporary-chat requests.
        inner[41] = [1]
        inner[45] = 1
    else:
        inner[41] = [2]


def _build_payload(prompt: str, model_id: int, think_mode: int, file_refs: list = None, extra_fields: dict = None) -> str:
    inner = [None] * 102
    if file_refs:
        refs = [[None, None, ref] for ref in file_refs]
        inner[0] = [prompt, 0, None, refs, None, None, 0]
    else:
        inner[0] = [prompt, 0, None, None, None, None, 0]
    inner[1] = ["en"]
    inner[2] = ["", "", "", None, None, None, None, None, None, ""]
    inner[6] = [0]
    inner[7] = 1
    inner[10] = 1
    inner[11] = 0
    inner[17] = [[think_mode]]
    inner[18] = 0
    inner[27] = 1
    inner[30] = [4]
    _apply_chat_persistence_flags(inner)
    inner[53] = 0
    inner[59] = str(uuid.uuid4())
    inner[61] = []
    inner[68] = 1
    inner[79] = model_id
    if extra_fields:
        for k, v in extra_fields.items():
            inner[k] = v
    outer = [None, json.dumps(inner)]
    params = {"f.req": json.dumps(outer)}
    if CONFIG.get("xsrf_token"):
        params["at"] = CONFIG["xsrf_token"]
    return urllib.parse.urlencode(params)


def _get_url() -> str:
    reqid = int(time.time()) % 1000000
    account_prefix = _account_prefix()
    return (
        f"https://gemini.google.com{account_prefix}/_/BardChatUi/data/"
        "assistant.lamda.BardFrontendService/StreamGenerate"
        f"?bl={CONFIG['gemini_bl']}&hl=en&_reqid={reqid}&rt=c"
    )


def clean_text(text: str, strip: bool = True) -> str:
    text = re.sub(
        r'```(?:python|javascript|text)\?code_(?:reference|stdout)&code_event_index=\d+\n.*?```\n?',
        '', text, flags=re.DOTALL
    )
    text = re.sub(r'http://googleusercontent\.com/card_content/\d+\n?', '', text)
    return text.strip() if strip else text


def _extract_texts_from_line(line: str) -> list:
    """Parse a single wrb.fr line and return list of text strings found."""
    if '"wrb.fr"' not in line or len(line) < 200:
        return []
    try:
        arr = json.loads(line)
        inner_str = arr[0][2]
        if not inner_str or len(inner_str) < 50:
            return []
        inner = json.loads(inner_str)
        if not (isinstance(inner, list) and len(inner) > 4 and inner[4]):
            return []
        texts = []
        for part in inner[4]:
            if isinstance(part, list) and len(part) > 1 and part[1] and isinstance(part[1], list):
                for t in part[1]:
                    if isinstance(t, str) and t:
                        texts.append(t)
        return texts
    except (json.JSONDecodeError, IndexError, TypeError):
        return []


def extract_response_text(raw: str) -> str:
    """Parse full response to get final text."""
    bard_err = re.search(r'BardErrorInfo\s*\[(\d+)\]', raw)
    if bard_err:
        raise UpstreamRejected(f"Gemini upstream rejected request: BardErrorInfo [{bard_err.group(1)}]")
    last_text = ""
    for line in raw.split("\n"):
        for t in _extract_texts_from_line(line):
            if len(t) > len(last_text):
                last_text = t
    return clean_text(last_text)


def generate(prompt: str, model_id: int, think_mode: int, file_refs: list = None, extra_fields: dict = None) -> str:
    """Non-streaming generation with retry."""
    body = _build_payload(prompt, model_id, think_mode, file_refs, extra_fields).encode()
    url = _get_url()
    headers = _build_headers()
    ctx = _get_ssl_ctx()
    proxy = CONFIG.get("proxy")

    if proxy and not _is_socks_proxy(proxy):
        # Stable proxy: reuse opener. {random} rotates credentials per request.
        if _RANDOM_TOKEN in proxy:
            proxy = _resolve_proxy(proxy)
            opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({"http": proxy, "https": proxy}),
                urllib.request.HTTPSHandler(context=_get_ssl_ctx())
            )
        else:
            opener = _get_http_proxy_opener(proxy)
    else:
        # Direct, or SOCKS proxy (which needs its own opener built lazily).
        opener = None

    last_err = None
    for attempt in range(CONFIG["retry_attempts"]):
        try:
            req = urllib.request.Request(url, data=body, headers=headers, method="POST")
            if _is_socks_proxy(proxy):
                resp = _get_socks_opener().open(req, timeout=CONFIG["request_timeout_sec"])
            elif opener is not None:
                resp = opener.open(req, timeout=CONFIG["request_timeout_sec"])
            else:
                resp = urllib.request.urlopen(req, context=ctx, timeout=CONFIG["request_timeout_sec"])
            with resp:
                chunks = []
                total = 0
                limit = max(1, int(CONFIG.get("max_upstream_response_bytes", 16 * 1024 * 1024)))
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > limit:
                        raise RuntimeError("upstream response too large")
                    chunks.append(chunk)
            raw = b"".join(chunks).decode("utf-8", errors="replace")
            return extract_response_text(raw)
        except Exception as e:
            last_err = e
            if attempt < CONFIG["retry_attempts"] - 1 and _retryable(e):
                log(f"Retry {attempt+1}/{CONFIG['retry_attempts']}: {e}")
                time.sleep(_retry_delay(attempt))
    raise last_err


def generate_stream(prompt: str, model_id: int, think_mode: int, file_refs: list = None, extra_fields: dict = None):
    """Streaming generation via httpx with retry on connection failure."""
    if not HAS_HTTPX:
        text = generate(prompt, model_id, think_mode, file_refs, extra_fields)
        if text:
            yield text
        return

    body = _build_payload(prompt, model_id, think_mode, file_refs, extra_fields)
    url = _get_url()
    headers = _build_headers()
    per_request_client = HAS_HTTPX and _RANDOM_TOKEN in (CONFIG.get("proxy") or "")
    client = _get_httpx_client()

    last_err = None
    emitted_raw_text = ""
    for attempt in range(CONFIG["retry_attempts"]):
        try:
            try:
                with client.stream("POST", url, content=body, headers=headers) as resp:
                    resp.raise_for_status()
                    buf = ""
                    total = 0
                    limit = max(1, int(CONFIG.get("max_upstream_response_bytes", 16 * 1024 * 1024)))
                    for chunk in resp.iter_text():
                        total += len(chunk.encode("utf-8"))
                        if total > limit:
                            raise RuntimeError("upstream response too large")
                        buf += chunk
                        if "BardErrorInfo" in buf:
                            bard_err = re.search(r'BardErrorInfo\s*\[(\d+)\]', buf)
                            if bard_err:
                                raise UpstreamRejected(
                                    f"Gemini upstream rejected request: BardErrorInfo [{bard_err.group(1)}]"
                                )
                        while "\n" in buf:
                            line, buf = buf.split("\n", 1)
                            for t in _extract_texts_from_line(line):
                                if t == emitted_raw_text or emitted_raw_text.startswith(t):
                                    continue
                                if not t.startswith(emitted_raw_text):
                                    raise UpstreamRejected("Gemini stream content changed during retry")
                                delta = clean_text(t[len(emitted_raw_text):], strip=False)
                                emitted_raw_text = t
                                if delta:
                                    yield delta
                return
            finally:
                if per_request_client:
                    client.close()
        except UpstreamRejected:
            # Gemini rejected the request itself (BardErrorInfo); a retry with
            # the same payload cannot succeed.
            raise
        except Exception as e:
            last_err = e
            if attempt < CONFIG["retry_attempts"] - 1 and _retryable(e):
                log(f"Stream retry {attempt+1}/{CONFIG['retry_attempts']}: {e}")
                time.sleep(_retry_delay(attempt))
    raise last_err
