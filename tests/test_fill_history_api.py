from unittest.mock import AsyncMock

import httpx
import pytest

from app.config import Settings
from app.okx import OkxError, OkxRestClient
from tests.test_ledger_repair import fill


@pytest.mark.parametrize(
    "method,path",
    [
        ("fills", "/api/v5/trade/fills"),
        ("fills_history", "/api/v5/trade/fills-history"),
    ],
)
async def test_fill_api_parameters_and_preserved_fields(method, path):
    requests = []

    def respond(r):
        requests.append(r)
        return httpx.Response(200, json={"code": "0", "data": [fill()]})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(respond), base_url="https://openapi.okx.com"
    ) as http:
        client = OkxRestClient(
            Settings(
                _env_file=None, okx_api_key="demo", okx_secret_key="demo", okx_passphrase="demo"
            ),
            http,
        )
        rows = await getattr(client, method)(
            "BTC-USDT-SWAP", after="bill-after", before="bill-before", limit=50
        )
    assert rows == [fill()]
    r = requests[0]
    assert r.method == "GET" and r.url.path == path
    assert dict(r.url.params) == {
        "instType": "SWAP",
        "instId": "BTC-USDT-SWAP",
        "after": "bill-after",
        "before": "bill-before",
        "limit": "50",
    }
    assert r.headers["x-simulated-trading"] == "1"


async def test_history_paginates_by_bill_id_with_explicit_window():
    client = OkxRestClient(Settings(_env_file=None))
    client.fills_history = AsyncMock(
        side_effect=[
            [{**fill("new"), "billId": "300"}, {**fill("middle"), "billId": "200"}],
            [{**fill("old"), "billId": "100"}],
        ]
    )
    try:
        rows = await client.fills_history_window(
            "BTC-USDT-SWAP", history_start_ms=100, history_end_ms=200, page_size=2
        )
        assert [r["tradeId"] for r in rows] == ["new", "middle", "old"]
        assert client.fills_history.await_args_list[1].kwargs["after"] == "200"
        assert client.fills_history.await_args_list[0].kwargs["begin"] == 100
        assert client.fills_history.await_args_list[0].kwargs["end"] == 200
    finally:
        await client.close()


@pytest.mark.parametrize("case", ["pages", "records", "cursor", "repeat", "oversized"])
async def test_history_bounds_fail_closed_instead_of_returning_partial_evidence(case):
    client = OkxRestClient(Settings(_env_file=None))
    page = [{**fill(), "billId": "100"}]
    if case == "cursor":
        page[0].pop("billId")
    if case == "oversized":
        page.append(fill("extra"))
    client.fills_history = AsyncMock(return_value=page)
    try:
        with pytest.raises(OkxError, match="evidence incomplete"):
            await client.fills_history_window(
                "BTC-USDT-SWAP",
                history_start_ms=100,
                history_end_ms=200,
                page_size=1,
                max_pages=1 if case == "pages" else 2,
                max_records=1 if case == "records" else 10,
            )
        assert client.fills_history.await_count <= 2
    finally:
        await client.close()


@pytest.mark.parametrize("status,code", [(503, "50026"), (401, "50113"), (429, "50011")])
async def test_history_http_errors_propagate_without_blind_retry(status, code):
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: requests.append(r) or httpx.Response(status, json={"code": code, "data": []})
        ),
        base_url="https://openapi.okx.com",
    ) as http:
        client = OkxRestClient(
            Settings(
                _env_file=None, okx_api_key="demo", okx_secret_key="demo", okx_passphrase="demo"
            ),
            http,
        )
        with pytest.raises(OkxError):
            await client.fills_history_window(
                "BTC-USDT-SWAP", history_start_ms=100, history_end_ms=200
            )
    assert len(requests) == 1 and requests[0].method == "GET"


async def test_history_http_timeout_propagates_without_trading_request():
    requests = []

    def timeout(r):
        requests.append(r)
        raise httpx.ReadTimeout("timeout", request=r)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(timeout), base_url="https://openapi.okx.com"
    ) as http:
        client = OkxRestClient(
            Settings(
                _env_file=None, okx_api_key="demo", okx_secret_key="demo", okx_passphrase="demo"
            ),
            http,
        )
        with pytest.raises(OkxError):
            await client.fills_history("BTC-USDT-SWAP")
    assert [r.method for r in requests] == ["GET"]
