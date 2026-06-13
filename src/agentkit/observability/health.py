"""Health checks + Prometheus / probes HTTP server.

Doc10 §9 maps each service kind to a (Liveness, Readiness) pair.
We expose:

* :class:`HealthCheck`            — one named check returning ok/fail.
* :class:`HealthRegistry`         — collects checks; ``run_all()`` /
  ``run_readiness()`` produce dict reports.
* :class:`ProbeServer`            — a tiny stdlib ``http.server``-based
  endpoint exposing ``/health``, ``/ready``, and ``/metrics`` (the
  Prometheus scrape).

We use ``http.server`` rather than FastAPI/Starlette because:

* Zero new heavy deps beyond what we already pull in.
* The probe endpoints do **trivial** work — no need for a full
  ASGI stack.
* It's trivial to swap to a richer server later if Doc09 control-
  plane API lands.
"""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    REGISTRY,
    generate_latest,
)

from agentkit.common.logging import get_logger

log = get_logger(__name__)

# A health check returns (ok, detail). Async-friendly.
CheckCallable = Callable[[], Awaitable[tuple[bool, str]]] | Callable[[], tuple[bool, str]]


# ============================================================
# HealthCheck + Registry
# ============================================================


@dataclass(frozen=True)
class HealthCheck:
    """A named health check.

    ``readiness=True`` checks count toward ``/ready``; otherwise
    they only count toward ``/health`` (liveness).
    """

    name: str
    fn: CheckCallable
    readiness: bool = False


class HealthRegistry:
    """Holds named :class:`HealthCheck` instances + runs them.

    A single global registry instance is exposed as
    :data:`health_registry` for convenience; tests can construct
    their own to avoid pollution.
    """

    def __init__(self) -> None:
        self._checks: dict[str, HealthCheck] = {}

    def register(
        self,
        name: str,
        fn: CheckCallable,
        *,
        readiness: bool = False,
    ) -> None:
        self._checks[name] = HealthCheck(name=name, fn=fn, readiness=readiness)

    def unregister(self, name: str) -> None:
        self._checks.pop(name, None)

    def names(self) -> list[str]:
        return sorted(self._checks)

    async def run_all(self) -> dict[str, Any]:
        """Run every check; return a JSON-friendly summary."""
        return await self._run_filtered(only_readiness=False)

    async def run_readiness(self) -> dict[str, Any]:
        """Run only the readiness-flagged checks."""
        return await self._run_filtered(only_readiness=True)

    async def _run_filtered(self, *, only_readiness: bool) -> dict[str, Any]:
        results: dict[str, Any] = {}
        ok_all = True
        for name, check in self._checks.items():
            if only_readiness and not check.readiness:
                continue
            try:
                ret = check.fn()
                if asyncio.iscoroutine(ret):
                    ok, detail = await ret
                else:
                    ok, detail = ret  # type: ignore[misc]
            except Exception as e:
                ok, detail = False, f"check raised: {e}"
            results[name] = {"ok": bool(ok), "detail": detail}
            ok_all = ok_all and bool(ok)
        return {"ok": ok_all, "checks": results}


# Single shared registry — modules call ``health_registry.register("foo", ...)``.
health_registry = HealthRegistry()


# ============================================================
# Probe HTTP server
# ============================================================


class _ProbeHandler(BaseHTTPRequestHandler):
    """Handles GET /metrics, /health, /ready.

    Bound to a ProbeServer instance via the ``server.registry``
    attribute (the Python stdlib server stores the user dict on
    the HTTPServer subclass).
    """

    server_version = "AgentKitProbe/1.0"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        # Route stdlib http.server's noisy default to our structured log.
        log.debug("probe.access", msg=format % args)

    def _write(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 — http.server convention
        path = self.path.split("?", 1)[0]
        if path == "/metrics":
            return self._handle_metrics()
        if path == "/health":
            return self._handle_health(readiness=False)
        if path == "/ready":
            return self._handle_health(readiness=True)
        self._write(404, b"not found\n", "text/plain")

    def _handle_metrics(self) -> None:
        try:
            body = generate_latest(REGISTRY)
        except Exception as e:
            self._write(500, f"metrics error: {e}".encode(), "text/plain")
            return
        self._write(200, body, CONTENT_TYPE_LATEST)

    def _handle_health(self, *, readiness: bool) -> None:
        registry: HealthRegistry = self.server.health_registry  # type: ignore[attr-defined]
        # Run the async checks in a fresh loop synchronously — each
        # request gets its own event loop so we don't conflict with
        # whatever loop the application runs.
        loop = asyncio.new_event_loop()
        try:
            report = loop.run_until_complete(
                registry.run_readiness() if readiness else registry.run_all(),
            )
        finally:
            loop.close()
        body = json.dumps(report, ensure_ascii=False).encode("utf-8")
        status = 200 if report["ok"] else 503
        self._write(status, body, "application/json")


@dataclass
class ProbeServer:
    """Tiny HTTP server exposing /metrics + /health + /ready.

    Use as a context manager or call ``start()`` / ``stop()``::

        with ProbeServer(port=9100) as probe:
            # business code emits metrics; Prometheus scrapes /metrics
            ...

    Or run alongside the main asyncio loop::

        probe = ProbeServer(port=9100)
        probe.start()
        try:
            await my_main()
        finally:
            probe.stop()
    """

    port: int = 9100
    host: str = "0.0.0.0"
    registry: HealthRegistry = field(default_factory=lambda: health_registry)

    _httpd: HTTPServer | None = field(default=None, init=False, repr=False)
    _thread: threading.Thread | None = field(default=None, init=False, repr=False)

    # ---- Lifecycle ----

    def start(self) -> None:
        if self._httpd is not None:
            return
        # Setting allow_reuse_address on the class is the documented
        # stdlib way to add SO_REUSEADDR — without it killing + restarting
        # the demo within ~30s of TCP TIME_WAIT fails with "Address already
        # in use".
        HTTPServer.allow_reuse_address = True
        httpd = HTTPServer((self.host, self.port), _ProbeHandler)
        # Stash the registry so the handler can access it.
        httpd.health_registry = self.registry  # type: ignore[attr-defined]
        thread = threading.Thread(
            target=httpd.serve_forever,
            name=f"agentkit-probe-{self.port}",
            daemon=True,
        )
        thread.start()
        self._httpd = httpd
        self._thread = thread
        log.info("probe.started", host=self.host, port=self.port)

    def stop(self) -> None:
        if self._httpd is None:
            return
        try:
            self._httpd.shutdown()
            self._httpd.server_close()
        finally:
            self._httpd = None
            self._thread = None
        log.info("probe.stopped", port=self.port)

    # ---- Context manager ----

    def __enter__(self) -> ProbeServer:
        self.start()
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.stop()

    # ---- Useful for tests ----

    @property
    def actual_port(self) -> int:
        """Return the port the server is actually bound to.

        Useful when ``port=0`` (let the OS pick a free port).
        """
        if self._httpd is None:
            raise RuntimeError("ProbeServer not started")
        return self._httpd.server_address[1]
