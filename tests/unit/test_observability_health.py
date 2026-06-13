"""Tests for the health checks + ProbeServer."""

from __future__ import annotations

import json

import httpx
import pytest

from agentkit.observability import (
    HealthRegistry,
    ProbeServer,
    metrics,
)


# ----------------------------------------------------------------
# HealthRegistry
# ----------------------------------------------------------------


class TestHealthRegistry:
    async def test_run_all_aggregates_results(self) -> None:
        reg = HealthRegistry()
        reg.register("a", lambda: (True, "ok"))
        reg.register("b", lambda: (True, "fine"))
        report = await reg.run_all()
        assert report["ok"] is True
        assert set(report["checks"]) == {"a", "b"}
        assert report["checks"]["a"]["ok"] is True

    async def test_failing_check_drops_overall(self) -> None:
        reg = HealthRegistry()
        reg.register("up", lambda: (True, "ok"))
        reg.register("down", lambda: (False, "broken"))
        report = await reg.run_all()
        assert report["ok"] is False
        assert report["checks"]["down"]["ok"] is False

    async def test_async_check_supported(self) -> None:
        reg = HealthRegistry()

        async def slow_check():
            return True, "async ok"

        reg.register("slow", slow_check)
        report = await reg.run_all()
        assert report["ok"]
        assert report["checks"]["slow"]["detail"] == "async ok"

    async def test_exception_treated_as_unhealthy(self) -> None:
        reg = HealthRegistry()

        def bomb():
            raise RuntimeError("boom")

        reg.register("bomb", bomb)
        report = await reg.run_all()
        assert report["ok"] is False
        assert "raised" in report["checks"]["bomb"]["detail"]

    async def test_readiness_filter(self) -> None:
        reg = HealthRegistry()
        reg.register("liveness_only", lambda: (True, "alive"))
        reg.register("readiness", lambda: (True, "ready"), readiness=True)
        # /health: both run.
        full = await reg.run_all()
        assert "liveness_only" in full["checks"]
        # /ready: only readiness checks.
        ready = await reg.run_readiness()
        assert "liveness_only" not in ready["checks"]
        assert "readiness" in ready["checks"]


# ----------------------------------------------------------------
# ProbeServer — actual HTTP roundtrip
# ----------------------------------------------------------------


class TestProbeServer:
    @pytest.fixture
    def reg(self) -> HealthRegistry:
        r = HealthRegistry()
        r.register("dummy", lambda: (True, "ok"))
        r.register("ready_dummy", lambda: (True, "ready"), readiness=True)
        return r

    def test_metrics_endpoint_returns_prometheus_format(self, reg) -> None:
        # Increment a metric so /metrics has content
        metrics.compile_total.labels(result="ok").inc()

        with ProbeServer(host="127.0.0.1", port=0, registry=reg) as probe:
            url = f"http://127.0.0.1:{probe.actual_port}/metrics"
            resp = httpx.get(url, timeout=2.0)
            assert resp.status_code == 200
            assert "compile_total" in resp.text
            # Prometheus exposition format always has these markers.
            assert "# HELP" in resp.text
            assert "# TYPE" in resp.text

    def test_health_endpoint_ok(self, reg) -> None:
        with ProbeServer(host="127.0.0.1", port=0, registry=reg) as probe:
            url = f"http://127.0.0.1:{probe.actual_port}/health"
            resp = httpx.get(url, timeout=2.0)
            assert resp.status_code == 200
            body = resp.json()
            assert body["ok"] is True
            assert "dummy" in body["checks"]

    def test_health_endpoint_503_on_failure(self) -> None:
        reg = HealthRegistry()
        reg.register("broken", lambda: (False, "down"))

        with ProbeServer(host="127.0.0.1", port=0, registry=reg) as probe:
            url = f"http://127.0.0.1:{probe.actual_port}/health"
            resp = httpx.get(url, timeout=2.0)
            assert resp.status_code == 503
            assert resp.json()["ok"] is False

    def test_ready_endpoint_filters_by_readiness(self, reg) -> None:
        with ProbeServer(host="127.0.0.1", port=0, registry=reg) as probe:
            url = f"http://127.0.0.1:{probe.actual_port}/ready"
            resp = httpx.get(url, timeout=2.0)
            assert resp.status_code == 200
            body = resp.json()
            # Only readiness-flagged checks are present.
            assert "ready_dummy" in body["checks"]
            assert "dummy" not in body["checks"]

    def test_unknown_path_404(self, reg) -> None:
        with ProbeServer(host="127.0.0.1", port=0, registry=reg) as probe:
            url = f"http://127.0.0.1:{probe.actual_port}/nope"
            resp = httpx.get(url, timeout=2.0)
            assert resp.status_code == 404
