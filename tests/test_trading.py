"""End-to-end tests: real TCP between the OMS and the mock broker on localhost."""
import asyncio
import os
import tempfile
import time
import unittest

from fixoms import fix
from fixoms.broker import MockBroker
from fixoms.client import OMS


async def wait_until(condition, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


class TradingTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.mkdtemp()
        self.broker = MockBroker(self.path("broker.json"), heartbeat=1, fill_delay=0.1,
                                 prices={"AAPL": 200.0})
        await self.broker.start()
        self.oms = self.start_oms()
        await wait_until(lambda: self.oms.session.logged_in)

    async def asyncTearDown(self):
        self.oms.close()
        await self.broker.stop()
        await asyncio.sleep(0.05)

    def path(self, name):
        return os.path.join(self.tmp, name)

    def start_oms(self):
        oms = OMS(self.path("oms.db"), self.path("client.json"), "127.0.0.1", self.broker.port, heartbeat=1)
        self.oms_task = asyncio.create_task(oms.run())
        return oms

    async def test_market_order_fills_in_partials(self):
        clordid = self.oms.place_order("AAPL", "buy", 300)
        await wait_until(lambda: self.oms.order(clordid)["status"] == "2")
        fills = self.oms.db.execute("SELECT qty FROM fills WHERE clordid = ?", (clordid,)).fetchall()
        self.assertEqual([q for (q,) in fills], [100, 100, 100])
        pos = self.oms.positions(marks={"AAPL": 210.0})["AAPL"]
        self.assertEqual(pos["qty"], 300)
        self.assertEqual(pos["avg_cost"], 200.0)
        self.assertEqual(pos["unrealized_pnl"], 3000.0)

    async def test_realized_pnl(self):
        buy = self.oms.place_order("AAPL", "buy", 300)
        await wait_until(lambda: self.oms.order(buy)["status"] == "2")
        self.broker.prices["AAPL"] = 205.0
        sell = self.oms.place_order("AAPL", "sell", 100)
        await wait_until(lambda: self.oms.order(sell)["status"] == "2")
        pos = self.oms.positions()["AAPL"]
        self.assertEqual(pos["qty"], 200)
        self.assertEqual(pos["realized_pnl"], 500.0)

    async def test_resting_limit_order_cancel(self):
        clordid = self.oms.place_order("AAPL", "buy", 100, price=150.0)  # below market, rests
        await wait_until(lambda: self.oms.order(clordid)["status"] == "0")
        self.oms.cancel(clordid)
        await wait_until(lambda: self.oms.order(clordid)["status"] == "4")

    async def test_cancel_after_fill_is_rejected(self):
        clordid = self.oms.place_order("AAPL", "buy", 100)
        await wait_until(lambda: self.oms.order(clordid)["status"] == "2")
        self.oms.cancel(clordid)
        await wait_until(lambda: any(d == "in" and m[35] == "9" for d, m in self.oms.session.history))
        self.assertEqual(self.oms.order(clordid)["status"], "2")

    async def test_disconnect_mid_fill_recovers_via_resend(self):
        clordid = self.oms.place_order("AAPL", "buy", 300)
        await wait_until(lambda: self.oms.order(clordid)["cum_qty"] == 100)
        self.oms.session.disconnect()  # remaining fills happen while we're offline
        await wait_until(lambda: self.oms.reconnects == 1 and self.oms.session.logged_in)
        await wait_until(lambda: self.oms.order(clordid)["status"] == "2")
        fills = self.oms.db.execute("SELECT COUNT(*), SUM(qty) FROM fills").fetchone()
        self.assertEqual(fills, (3, 300))
        sent = self.oms.session.history
        self.assertTrue(any(d == "out" and m[35] == "2" for d, m in sent), "client sent ResendRequest")
        self.assertTrue(any(d == "in" and m.get(43) == "Y" and m[35] == "8" for d, m in sent),
                        "broker resent execution reports with PossDupFlag")

    async def test_duplicate_execution_report_ignored(self):
        clordid = self.oms.place_order("AAPL", "buy", 300)
        await wait_until(lambda: self.oms.order(clordid)["status"] == "2")
        fill = next(m for d, m in self.oms.session.history if d == "in" and m.get(150) == "F")
        self.oms.on_app(dict(fill))  # same ExecID delivered again
        self.assertEqual(self.oms.db.execute("SELECT COUNT(*) FROM fills").fetchone()[0], 3)

    async def test_sequence_numbers_survive_restart(self):
        clordid = self.oms.place_order("AAPL", "buy", 300)
        await wait_until(lambda: self.oms.order(clordid)["cum_qty"] == 100)
        self.oms.stop()  # "crash" the client process
        await self.oms_task
        next_out = self.oms.session.next_out
        self.oms.close()

        self.oms = self.start_oms()  # new process, same files
        self.assertEqual(self.oms.session.next_out, next_out)
        await wait_until(lambda: self.oms.session.logged_in)
        await wait_until(lambda: self.oms.order(clordid)["status"] == "2")
        self.assertEqual(self.oms.db.execute("SELECT SUM(qty) FROM fills").fetchone()[0], 300)

    async def test_silent_peer_gets_test_request_then_dropped(self):
        broker_session = self.broker.session
        broker_session._write = lambda raw: None  # broker goes silent
        await wait_until(lambda: any(d == "out" and m[35] == "1" for d, m in self.oms.session.history))
        await wait_until(lambda: self.oms.reconnects >= 1, timeout=6)

    async def test_test_request_answered_with_matching_heartbeat(self):
        self.oms.session.send("1", [(112, "ping-42")])
        await wait_until(lambda: any(d == "in" and m[35] == "0" and m.get(112) == "ping-42"
                                     for d, m in self.oms.session.history))

    async def test_garbled_message_dropped_without_consuming_seq(self):
        next_in = self.broker.session.next_in
        bad = fix.encode("0", "CLIENT", "BROKER", next_in).replace(b"35=0", b"35=1")  # wrong checksum
        self.oms.session.writer.write(bad)
        await asyncio.sleep(0.2)
        self.assertEqual(self.broker.session.next_in, next_in)
        self.assertTrue(self.broker.session.connected)

    async def test_order_rejected_when_not_logged_on(self):
        self.oms.stop()
        await self.oms_task
        with self.assertRaises(RuntimeError):
            self.oms.place_order("AAPL", "buy", 100)


if __name__ == "__main__":
    unittest.main()
