"""Order management client (FIX initiator): orders, fills, positions, reconnects and reconciliation."""
import asyncio
import logging
import sqlite3
import uuid

from . import fix
from .session import Session

log = logging.getLogger("oms")

OPEN_STATUSES = ("A", "0", "1")  # PendingNew, New, PartiallyFilled
STATUS_NAMES = {"A": "PendingNew", "0": "New", "1": "PartiallyFilled", "2": "Filled",
                "4": "Canceled", "8": "Rejected"}
SIDES = {"buy": "1", "sell": "2"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    clordid TEXT PRIMARY KEY, order_id TEXT, symbol TEXT NOT NULL, side TEXT NOT NULL,
    qty REAL NOT NULL, ord_type TEXT NOT NULL, price REAL, status TEXT NOT NULL,
    cum_qty REAL NOT NULL DEFAULT 0, avg_px REAL NOT NULL DEFAULT 0, text TEXT
);
CREATE TABLE IF NOT EXISTS fills (
    exec_id TEXT PRIMARY KEY, clordid TEXT NOT NULL REFERENCES orders(clordid),
    symbol TEXT NOT NULL, side TEXT NOT NULL, qty REAL NOT NULL, price REAL NOT NULL, ts TEXT
);
CREATE INDEX IF NOT EXISTS orders_status ON orders(status);
"""


class OMS:
    def __init__(self, db_path, store_path, host, port, heartbeat=30):
        self.db = sqlite3.connect(db_path)
        self.db.executescript(SCHEMA)
        self.session = Session("CLIENT", "BROKER", store_path, on_app=self.on_app,
                               on_logon=self.reconcile, heartbeat=heartbeat)
        self.host, self.port = host, port
        self.reconnects = 0
        self._running = False

    # --- connectivity ------------------------------------------------------

    async def run(self):
        """Connect and stay connected, reconnecting with exponential backoff."""
        self._running = True
        backoff = 0.5
        while self._running:
            try:
                reader, writer = await asyncio.open_connection(self.host, self.port)
            except OSError as e:
                log.warning("connect failed (%s), retrying in %.1fs", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10)
                continue
            backoff = 0.5
            await self.session.run(reader, writer, initiate=True)
            if self._running:
                self.reconnects += 1
                log.warning("connection lost, reconnecting")
                await asyncio.sleep(backoff)

    def stop(self):
        self._running = False
        self.session.disconnect()

    def close(self):
        self.stop()
        self.db.close()

    def reconcile(self):
        """After every logon, ask the broker for the state of every order we think is open."""
        for clordid, symbol, side in self.db.execute(
                f"SELECT clordid, symbol, side FROM orders WHERE status IN {OPEN_STATUSES}"):
            self.session.send("H", [(11, clordid), (55, symbol), (54, side)])

    # --- orders ------------------------------------------------------------

    def place_order(self, symbol, side, qty, price=None):
        # Refuse rather than queue: an order resent minutes later at a stale price is worse than a rejection.
        if not self.session.logged_in:
            raise RuntimeError("not logged on to broker")
        clordid = uuid.uuid4().hex[:12]
        ord_type = "2" if price else "1"
        self.db.execute("INSERT INTO orders (clordid, symbol, side, qty, ord_type, price, status) "
                        "VALUES (?, ?, ?, ?, ?, ?, 'A')", (clordid, symbol, SIDES[side], qty, ord_type, price))
        self.db.commit()
        body = [(11, clordid), (55, symbol), (54, SIDES[side]), (38, qty), (40, ord_type),
                (59, 0), (60, fix.utc_now())]
        if price:
            body.append((44, price))
        self.session.send("D", body)
        return clordid

    def cancel(self, clordid):
        if not self.session.logged_in:
            raise RuntimeError("not logged on to broker")
        symbol, side, qty = self.db.execute(
            "SELECT symbol, side, qty FROM orders WHERE clordid = ?", (clordid,)).fetchone()
        cancel_id = uuid.uuid4().hex[:12]
        self.session.send("F", [(11, cancel_id), (41, clordid), (55, symbol), (54, side),
                                (38, qty), (60, fix.utc_now())])
        return cancel_id

    def order(self, clordid):
        row = self.db.execute("SELECT clordid, symbol, side, qty, status, cum_qty, avg_px, text "
                              "FROM orders WHERE clordid = ?", (clordid,)).fetchone()
        keys = ("clordid", "symbol", "side", "qty", "status", "cum_qty", "avg_px", "text")
        return dict(zip(keys, row)) if row else None

    # --- incoming ----------------------------------------------------------

    def on_app(self, msg):
        if msg[35] == "8":
            self._on_execution_report(msg)
        elif msg[35] == "9":
            log.warning("cancel rejected for %s: %s", msg.get(41), msg.get(58))
        else:
            log.warning("unhandled message type %s: %s", msg[35], msg.get(58))

    def _on_execution_report(self, msg):
        clordid = msg.get(41) or msg[11]  # cancels reference the original order via OrigClOrdID
        row = self.db.execute("SELECT cum_qty FROM orders WHERE clordid = ?", (clordid,)).fetchone()
        if row is None:
            log.warning("execution report for unknown order %s", clordid)
            return
        exec_type, cum_qty = msg[150], float(msg[14])

        if exec_type == "F":
            inserted = self.db.execute(
                "INSERT OR IGNORE INTO fills VALUES (?, ?, ?, ?, ?, ?, ?)",
                (msg[17], clordid, msg[55], msg[54], float(msg[32]), float(msg[31]), msg.get(52))).rowcount
            if not inserted:
                log.info("duplicate fill %s ignored", msg[17])
                return
        elif exec_type == "I" and cum_qty != row[0]:
            log.warning("reconciliation mismatch on %s: ours cum=%s broker cum=%s", clordid, row[0], cum_qty)

        if cum_qty < row[0]:
            log.info("stale report for %s ignored", clordid)
            return
        self.db.execute("UPDATE orders SET status = ?, cum_qty = ?, avg_px = ?, order_id = ?, text = ? "
                        "WHERE clordid = ?",
                        (msg[39], cum_qty, float(msg[6]), msg.get(37), msg.get(58), clordid))
        self.db.commit()
        log.info("%s %s: %s cum=%s avg=%s", clordid, STATUS_NAMES.get(msg[39], msg[39]),
                 msg[55], cum_qty, msg[6])

    # --- positions ---------------------------------------------------------

    def positions(self, marks=None):
        """Net position, average cost and realized P&L per symbol (average-cost method)."""
        book = {}
        for symbol, side, qty, price in self.db.execute(
                "SELECT symbol, side, qty, price FROM fills ORDER BY rowid"):
            p = book.setdefault(symbol, {"qty": 0.0, "avg_cost": 0.0, "realized_pnl": 0.0})
            signed = qty if side == "1" else -qty
            if p["qty"] == 0 or (p["qty"] > 0) == (signed > 0):
                p["avg_cost"] = (p["avg_cost"] * abs(p["qty"]) + price * qty) / (abs(p["qty"]) + qty)
                p["qty"] += signed
                continue
            closed = min(qty, abs(p["qty"]))
            p["realized_pnl"] += closed * (price - p["avg_cost"]) * (1 if p["qty"] > 0 else -1)
            p["qty"] += signed
            if p["qty"] == 0:
                p["avg_cost"] = 0.0
            elif qty > closed:  # flipped from long to short or vice versa
                p["avg_cost"] = price
        for symbol, p in book.items():
            mark = (marks or {}).get(symbol)
            p["unrealized_pnl"] = (mark - p["avg_cost"]) * p["qty"] if mark is not None else None
        return book
