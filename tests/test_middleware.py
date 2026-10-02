import re

from fastapi.testclient import TestClient

from app import main


class FakePool:
    async def forward(self, payload, request_id=None):
        return 200, {"completion": "ok"}, "http://fake:9000"

    async def aclose(self):
        pass


def test_request_id_header_and_log(caplog):
    with TestClient(main.app) as client:
        main.app.state.pool = FakePool()
        with caplog.at_level("INFO", logger="llm-router.requests"):
            resp = client.post("/v1/generate", json={"prompt": "hi"})
    assert re.fullmatch(r"[0-9a-f]{12}", resp.headers["x-request-id"])
    line = "".join(caplog.messages)
    assert "path=/v1/generate" in line
    assert "status=200" in line
    assert "duration_ms=" in line


def test_store_initialized_on_state():
    with TestClient(main.app) as client:
        conversation = main.app.state.store.create_conversation()
        assert conversation["id"]
