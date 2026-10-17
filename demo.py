"""Runs the broker and OMS in one process and walks through a trading session with a dropped connection.

    python demo.py          # summary logs
    python demo.py -v       # also print every raw FIX message
"""
import asyncio
import logging
import os
import sys
import tempfile

from fixoms.broker import MockBroker
from fixoms.client import OMS, STATUS_NAMES


async def wait_for(condition, timeout=10):
    async with asyncio.timeout(timeout):
        while not condition():
            await asyncio.sleep(0.05)


async def main():
    sys.stdout.reconfigure(line_buffering=True)
    logging.basicConfig(level=logging.DEBUG if "-v" in sys.argv else logging.INFO,
                        format="%(asctime)s %(name)-16s %(message)s", datefmt="%H:%M:%S")
    tmp = tempfile.mkdtemp()
    broker = MockBroker(os.path.join(tmp, "broker.json"), heartbeat=2, fill_delay=0.5,
                        prices={"AAPL": 200.0, "MSFT": 400.0})
    await broker.start()
    oms = OMS(os.path.join(tmp, "oms.db"), os.path.join(tmp, "client.json"), "127.0.0.1", broker.port, heartbeat=2)
    asyncio.create_task(oms.run())
    await wait_for(lambda: oms.session.logged_in)

    print("\n=== 1. Market order, filled in three partials ===")
    buy = oms.place_order("AAPL", "buy", 300)
    await wait_for(lambda: oms.order(buy)["cum_qty"] >= 100)

    print("\n=== 2. Connection drops after the first fill; broker keeps filling ===")
    oms.session.disconnect()
    await wait_for(lambda: oms.reconnects == 1 and oms.session.logged_in)
    await wait_for(lambda: oms.order(buy)["status"] == "2")

    print("\n=== 3. Resting limit order, then cancel ===")
    limit = oms.place_order("MSFT", "buy", 50, price=390.0)
    await wait_for(lambda: oms.order(limit)["status"] == "0")
    oms.cancel(limit)
    await wait_for(lambda: oms.order(limit)["status"] == "4")

    print("\n=== 4. Price moves, sell part of the position ===")
    broker.prices["AAPL"] = 206.0
    sell = oms.place_order("AAPL", "sell", 100)
    await wait_for(lambda: oms.order(sell)["status"] == "2")

    print("\n=== Orders ===")
    for clordid, symbol, side, qty, status, cum, avg in oms.db.execute(
            "SELECT clordid, symbol, side, qty, status, cum_qty, avg_px FROM orders"):
        print(f"  {clordid}  {symbol:5} {'BUY ' if side == '1' else 'SELL'} {qty:>5.0f}  "
              f"{STATUS_NAMES[status]:16} filled={cum:.0f} avg={avg:.2f}")
    print("\n=== Positions (marked at broker price) ===")
    for symbol, p in oms.positions(marks=broker.prices).items():
        print(f"  {symbol:5} qty={p['qty']:.0f} avg_cost={p['avg_cost']:.2f} "
              f"realized={p['realized_pnl']:.2f} unrealized={p['unrealized_pnl']:.2f}")

    oms.close()
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
