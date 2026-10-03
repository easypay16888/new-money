from __future__ import annotations

from contextlib import asynccontextmanager
from secrets import compare_digest

from fastapi import Depends, FastAPI, Header, HTTPException, Response

from app.config import Mode, Settings
from app.models import GovernorState
from app.runtime import TradingRuntime


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    runtime = TradingRuntime(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await runtime.start()
        try:
            yield
        finally:
            if runtime.running:
                await runtime.stop()
            await runtime.close()

    app = FastAPI(title="OKX Quant Control API", lifespan=lifespan)
    app.state.runtime = runtime

    async def authorize(authorization: str | None = Header(default=None)) -> None:
        if settings.mode == Mode.LIVE and (
            not settings.api_token or not matches_token(authorization, settings.api_token)
        ):
            raise HTTPException(status_code=401, detail="authorization required")

    def matches_token(authorization: str | None, token: str) -> bool:
        return bool(token and authorization and compare_digest(
            authorization.encode(), ("Bearer " + token).encode()
        ))

    async def authorize_status(authorization: str | None = Header(default=None)) -> None:
        if settings.mode == Mode.LIVE and not (
            matches_token(authorization, settings.api_token)
            or matches_token(authorization, settings.status_api_token)
        ):
            raise HTTPException(status_code=401, detail="authorization required")

    @app.get("/health")
    async def health() -> dict:
        return {
            "running": runtime.running,
            "risk_state": runtime.governor.state,
            "database": runtime.store.healthy,
            "redis": runtime.redis is not None,
        }

    @app.get("/metrics", dependencies=[Depends(authorize)])
    async def metrics() -> Response:
        runtime.metrics.update(
            runtime.portfolio,
            runtime.governor.state,
            {str(i): ws.reconnects for i, ws in enumerate(runtime.sockets)},
            stale=any(not ws.is_fresh() for ws in runtime.sockets),
            trade_count=len(runtime.order_manager.seen_trade_ids),
        )
        return Response(runtime.metrics.render(), media_type="text/plain; version=0.0.4")

    @app.get("/status", dependencies=[Depends(authorize_status)])
    async def status() -> dict:
        return {
            "mode": settings.mode,
            "running": runtime.running,
            "risk_state": runtime.governor.state,
            "reason": runtime.governor.reason,
            "synchronized": runtime.portfolio.synchronized,
            "live_writer_lease": {
                "required": settings.mode == Mode.LIVE,
                "held": runtime.live_lease is not None and runtime.live_lease.held,
            },
            "database_backend": runtime.store.engine.dialect.name,
            "blocked_symbols": sorted(runtime.entry_controller.blocked),
            "emergency_targets": {
                symbol: str(target) for symbol, target in runtime.emergency.targets.items()
            },
            "auto_recovery": runtime.auto_recovery_status(),
            "protective_algos": list(runtime.algo_manager.algos.values()),
            "websockets": [
                {
                    "url": ws.url,
                    "connected": ws.connected,
                    "fresh": ws.is_fresh(),
                    "reconnects": ws.reconnects,
                }
                for ws in runtime.sockets
            ],
        }

    @app.get("/market", dependencies=[Depends(authorize)])
    async def market() -> dict:
        return {
            symbol: tick.model_dump(mode="json")
            for symbol, tick in runtime.market.latest_ticks.items()
        }

    @app.get("/positions", dependencies=[Depends(authorize)])
    async def positions() -> dict:
        return runtime.portfolio.model_dump(mode="json")

    @app.get("/orders", dependencies=[Depends(authorize)])
    async def orders() -> list[dict]:
        return list(runtime.order_manager.orders.values())

    @app.get("/signals", dependencies=[Depends(authorize)])
    async def signals() -> list[dict]:
        return await runtime.store.latest("signals")

    @app.get("/risk", dependencies=[Depends(authorize)])
    async def risk() -> dict:
        return {
            "state": runtime.governor.state,
            "reason": runtime.governor.reason,
            "limits": {
                "risk_per_trade": settings.risk_per_trade,
                "max_total_open_risk": settings.max_total_open_risk,
                "max_daily_loss": settings.max_daily_loss,
                "max_weekly_drawdown": settings.max_weekly_drawdown,
            },
        }

    @app.get("/performance", dependencies=[Depends(authorize)])
    async def performance() -> dict:
        return {
            "equity": str(runtime.portfolio.equity),
            "daily_pnl": str(runtime.portfolio.daily_pnl),
        }

    @app.post("/system/start", dependencies=[Depends(authorize)])
    async def start() -> dict:
        await runtime.start()
        return {"running": runtime.running}

    @app.post("/system/stop", dependencies=[Depends(authorize)])
    async def stop() -> dict:
        await runtime.stop()
        return {"running": runtime.running}

    @app.post("/system/halt", dependencies=[Depends(authorize)])
    async def halt() -> dict:
        await runtime.enter_halt("manual kill switch")
        return {"state": runtime.governor.state}

    @app.post("/system/resume", dependencies=[Depends(authorize)])
    async def resume() -> dict:
        if not await runtime.resume():
            raise HTTPException(
                status_code=409, detail="cannot resume until all dependencies and state are healthy"
            )
        return {"state": runtime.governor.state}

    @app.post("/orders/cancel-all", dependencies=[Depends(authorize)])
    async def cancel_all() -> dict:
        await runtime.enter_halt("manual cancel all")
        return {"confirmed": not runtime.entry_controller.blocked, "risk_state": GovernorState.HALT}

    return app


app = create_app()
