"""Configuration management."""
import json
import os

DEFAULT_CONFIG = {
    "port": 8081,
    "host": "0.0.0.0",
    "retry_attempts": 3,
    "retry_delay_sec": 2,
    "request_timeout_sec": 180,
    "max_request_body_bytes": 4 * 1024 * 1024,
    "max_upstream_response_bytes": 16 * 1024 * 1024,
    # High-concurrency profile (anonymous + per-request IP pool).
    # Tuned for throughput over latency: requests queue instead of failing fast.
    "max_inflight_requests": 64,
    "max_waiting_requests": 192,
    "inflight_acquire_timeout_sec": 45.0,
    "max_concurrent_connections": 320,
    "http_listen_backlog": 128,
    "stream_flush_interval_bytes": 0,
    "gemini_bl": "boq_assistant-bard-web-server_20260716.08_p0",
    "auth_user": None,
    "xsrf_token": None,
    "default_model": "gemini-3.6-flash",
    "log_requests": True,
    "cookie_file": None,
    "proxy": None,
    "cache_db_path": "./data/gemini-web2api-cache.sqlite3",
    "cache_default_ttl_sec": 3600,
    "cache_max_ttl_sec": 604800,
    "auto_cache": True,
    "api_keys": [],
    "temporary_chats": False,
}

CONFIG = dict(DEFAULT_CONFIG)


def load_config(path: str = None):
    """Load config from JSON file."""
    if path and os.path.exists(path):
        with open(path) as f:
            CONFIG.update(json.load(f))
    return CONFIG


def find_config():
    """Search for config file in standard locations."""
    for p in ["./config.json", os.path.expanduser("~/.config/gemini-web2api/config.json")]:
        if os.path.exists(p):
            return p
    return None
