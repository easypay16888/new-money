from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal

from app.backtest import EventDrivenBacktester, run_walk_forward, walk_forward_indices
from app.config import Settings
from app.models import Candle
from app.okx import OkxRestClient
from app.storage import Store


async def run_backtest(symbol: str, equity: Decimal) -> None:
    settings = Settings()
    store = Store(settings.database_url)
    client = OkxRestClient(settings)
    try:
        await store.initialize()
        rows = await store.latest("market_candles", limit=100000)
        candles = sorted(
            (
                Candle.model_validate(row)
                for row in rows
                if row.get("symbol") == symbol and row.get("timeframe") == "15m"
            ),
            key=lambda item: item.timestamp,
        )
        if len(candles) < 201:
            raise ValueError("backtest requires at least 201 confirmed 15m candles")
        context = {
            timeframe: sorted(
                (
                    Candle.model_validate(row)
                    for row in rows
                    if row.get("symbol") == symbol and row.get("timeframe") == timeframe
                ),
                key=lambda item: item.timestamp,
            )
            for timeframe in ("1H", "4H", "5m")
        }
        instruments = await client.instruments()
        if symbol not in instruments:
            raise ValueError("instrument not available")
        result = await EventDrivenBacktester(settings, instruments[symbol]).run(
            candles, equity, context=context
        )
        metrics = result.metrics()
        await store.append(
            "backtest_runs",
            {"symbol": symbol, "metrics": metrics, "params": result.params, "bars": len(candles)},
        )
        print(json.dumps(metrics, indent=2))
    finally:
        await client.close()
        await store.close()


async def run_walk_forward_command(
    symbol: str, equity: Decimal, train: int, validation: int, out_of_sample: int
) -> None:
    settings = Settings()
    store = Store(settings.database_url)
    client = OkxRestClient(settings)
    try:
        await store.initialize()
        rows = await store.latest("market_candles", limit=100000)
        candles = sorted(
            (
                Candle.model_validate(row)
                for row in rows
                if row.get("symbol") == symbol and row.get("timeframe") == "15m"
            ),
            key=lambda item: item.timestamp,
        )
        instruments = await client.instruments()
        context = {
            timeframe: sorted(
                (
                    Candle.model_validate(row)
                    for row in rows
                    if row.get("symbol") == symbol and row.get("timeframe") == timeframe
                ),
                key=lambda item: item.timestamp,
            )
            for timeframe in ("1H", "4H", "5m")
        }
        folds = await run_walk_forward(
            candles,
            settings,
            instruments[symbol],
            equity,
            train,
            validation,
            out_of_sample,
            context=context,
        )
        if not folds:
            raise ValueError("not enough candles for one walk-forward fold")
        await store.append(
            "backtest_runs", {"symbol": symbol, "walk_forward": folds, "bars": len(candles)}
        )
        print(json.dumps(folds, indent=2))
    finally:
        await client.close()
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="OKX quant offline tools")
    subparsers = parser.add_subparsers(dest="command", required=True)
    backtest = subparsers.add_parser("backtest")
    backtest.add_argument("--symbol", default="BTC-USDT-SWAP")
    backtest.add_argument("--equity", type=Decimal, default=Decimal("10000"))
    windows = subparsers.add_parser("walk-forward")
    windows.add_argument("--bars", type=int)
    windows.add_argument("--run", action="store_true")
    windows.add_argument("--symbol", default="BTC-USDT-SWAP")
    windows.add_argument("--equity", type=Decimal, default=Decimal("10000"))
    windows.add_argument("--train", type=int, required=True)
    windows.add_argument("--validation", type=int, required=True)
    windows.add_argument("--out-of-sample", type=int, required=True)
    args = parser.parse_args()
    if args.command == "backtest":
        asyncio.run(run_backtest(args.symbol, args.equity))
    elif args.run:
        asyncio.run(
            run_walk_forward_command(
                args.symbol, args.equity, args.train, args.validation, args.out_of_sample
            )
        )
    else:
        if args.bars is None:
            parser.error("--bars is required when --run is absent")
        for train, validation, oos in walk_forward_indices(
            args.bars, args.train, args.validation, args.out_of_sample
        ):
            print(
                f"train={train.start}:{train.stop} validation={validation.start}:{validation.stop} oos={oos.start}:{oos.stop}"
            )


if __name__ == "__main__":
    main()
