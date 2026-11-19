"""Day 14 tests, HTTP load test script and real benchmark numbers.

The percentile math is checked against known values, argument
validation is checked to fail fast, and small in process load
tests run against the real app to prove the script drives the
HTTP API, counts ops, and verifies the values it wrote. The runs
here are tiny on purpose, the real numbers live in
benchmarks/results.md, measured with the script itself.
"""
import json

import pytest
from fastapi.testclient import TestClient

from app.main import app
from benchmarks.load_test import (
    main as load_test_main,
)
from benchmarks.load_test import (
    percentile,
    run_load_test,
    summarize_latencies,
)

client = TestClient(app)


def test_percentile_known_values():
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    assert percentile(values, 0) == 10
    assert percentile(values, 50) == 55
    assert percentile(values, 100) == 100
    assert percentile(values, 90) == 91
    assert percentile([7], 50) == 7


def test_percentile_rejects_empty_list_and_bad_pct():
    with pytest.raises(ValueError):
        percentile([], 50)
    with pytest.raises(ValueError):
        percentile([1, 2, 3], -1)
    with pytest.raises(ValueError):
        percentile([1, 2, 3], 101)


def test_summarize_latencies_reports_ms():
    summary = summarize_latencies([0.001, 0.002, 0.003, 0.004])
    assert summary["count"] == 4
    assert summary["mean_ms"] == 2.5
    assert summary["p50_ms"] == 2.5
    assert summary["p99_ms"] == 3.97
    assert summary["max_ms"] == 4.0
    with pytest.raises(ValueError):
        summarize_latencies([])


def test_run_load_test_rejects_bad_arguments():
    with pytest.raises(ValueError):
        run_load_test(app=app, threads=0)
    with pytest.raises(ValueError):
        run_load_test(app=app, ops_per_thread=0)
    with pytest.raises(ValueError):
        run_load_test(app=app, read_ratio=1.5)
    with pytest.raises(ValueError):
        run_load_test(app=app, read_ratio=-0.1)
    with pytest.raises(ValueError):
        run_load_test(app=app, keys=0)
    with pytest.raises(ValueError):
        run_load_test(app=app, value_size=0)
    with pytest.raises(ValueError):
        run_load_test(app=app, distribution="zipf")


def test_small_disjoint_load_test_is_correct():
    result = run_load_test(
        app=app,
        threads=2,
        ops_per_thread=50,
        read_ratio=0.5,
        keys=20,
        value_size=20,
        distribution="disjoint",
    )
    assert result["mode"] == "in-process"
    assert result["ops"] == 100
    assert result["reads"] + result["writes"] == 100
    assert result["errors"] == []
    assert result["wrong_values"] == 0
    assert result["correct"] is True
    latency = result["latency_ms"]
    assert latency["count"] == 100
    assert latency["p95_ms"] >= latency["p50_ms"] >= 0
    assert latency["max_ms"] >= latency["p99_ms"]
    assert result["ops_per_second"] > 0
    assert result["hit_rate"] is not None
    assert 0 <= result["hit_rate"] <= 1


def test_small_shared_load_test_counts_only_statuses():
    result = run_load_test(
        app=app,
        threads=2,
        ops_per_thread=50,
        read_ratio=0.5,
        keys=10,
        value_size=20,
        distribution="shared",
    )
    assert result["distribution"] == "shared"
    assert result["ops"] == 100
    assert result["errors"] == []
    assert result["wrong_values"] == 0
    assert result["correct"] is True


def test_write_only_load_test_has_no_reads():
    result = run_load_test(
        app=app,
        threads=1,
        ops_per_thread=20,
        read_ratio=0.0,
        keys=5,
        value_size=10,
        distribution="disjoint",
    )
    assert result["reads"] == 0
    assert result["writes"] == 20
    assert result["correct"] is True


def test_load_test_main_writes_json_output(tmp_path):
    output = tmp_path / "load-result.json"
    code = load_test_main(
        ["-t", "1", "-n", "10", "-r", "0.5", "-k", "5",
         "-v", "10", "-o", str(output)]
    )
    assert code == 0
    saved = json.loads(output.read_text())
    assert saved["ops"] == 10
    assert saved["correct"] is True
    assert saved["latency_ms"]["count"] == 10


def test_load_test_main_rejects_bad_read_ratio():
    with pytest.raises(SystemExit):
        load_test_main(["-r", "2"])


def test_small_live_server_load_test():
    """The -u path drives a real server over HTTP, direct, no proxy.

    This is the path the proxy environment broke: a plain
    httpx.Client reads proxy variables and chokes on the no_proxy
    list, so the script builds its live client with trust_env
    False. The server runs in a thread on a scratch port, and the
    run is tiny, so it stays fast.
    """
    import threading
    import time

    import httpx
    import uvicorn

    config = uvicorn.Config(
        app, host="127.0.0.1", port=8129, log_level="error"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        probe = httpx.Client(timeout=5.0, trust_env=False)
        try:
            for _ in range(50):
                try:
                    if probe.get("http://127.0.0.1:8129/health").status_code == 200:
                        break
                except Exception:
                    time.sleep(0.1)
            else:
                pytest.fail("live test server did not come up")
        finally:
            probe.close()
        result = run_load_test(
            base_url="http://127.0.0.1:8129",
            threads=2,
            ops_per_thread=25,
            read_ratio=0.5,
            keys=10,
            value_size=20,
            distribution="disjoint",
        )
        assert result["mode"] == "live server"
        assert result["ops"] == 50
        assert result["errors"] == []
        assert result["wrong_values"] == 0
        assert result["correct"] is True
    finally:
        server.should_exit = True
        thread.join(timeout=10)
