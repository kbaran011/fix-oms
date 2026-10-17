"""Mock broker (FIX acceptor). Fills marketable orders in chunks so partial fills happen."""
import asyncio
import uuid

from .session import Session

FINAL = {"2", "4", "8"}  # Filled, Canceled, Rejected


class MockBroker:
    def __init__(self, store_path, host="127.0.0.1", port=0, heartbeat=30,
                 fill_delay=0.2, fill_chunks=3, prices=None):
        self.session = Session("BROKER", "CLIENT", store_path, on_app=self.on_app, heartbeat=heartbeat)
        self.host, self.port = host, port
        self.fill_delay = fill_delay
        self.fill_chunks = fill_chunks
        self.prices = prices if prices is not None else {}
        self.orders = {}

    async def start(self):
        self.server = await asyncio.start_server(self._on_connect, self.host, self.port)
        self.port = self.server.sockets[0].getsockname()[1]

    async def stop(self):
        self.server.close()
        self.session.disconnect()
        await self.server.wait_closed()

    async def _on_connect(self, reader, writer):
        if self.session.connected:
            writer.close()  # one client session at a time
            return
        await self.session.run(reader, writer)

    def price(self, symbol):
        return self.prices.get(symbol, 100.0)

    def on_app(self, msg):
        handlers = {"D": self._new_order, "F": self._cancel, "H": self._status}
        handler = handlers.get(msg[35])
        if handler:
            handler(msg)
        else:  # BusinessMessageReject
            self.session.send("j", [(45, msg[34]), (372, msg[35]), (380, 3), (58, "unsupported message type")])

    def _new_order(self, msg):
        clordid = msg[11]
        if clordid in self.orders:
            dup = dict(self.orders[clordid])
            dup["status"] = "8"
            return self._report(dup, "8", text="duplicate ClOrdID")
        order = {
            "clordid": clordid, "order_id": f"O{len(self.orders) + 1}", "symbol": msg[55], "side": msg[54],
            "qty": int(float(msg[38])), "ord_type": msg[40], "price": float(msg.get(44, 0)),
            "cum": 0, "notional": 0.0, "status": "0",
        }
        self.orders[clordid] = order
        self._report(order, "0")
        if self._marketable(order):
            asyncio.get_running_loop().create_task(self._fill(order))

    def _marketable(self, order):
        if order["ord_type"] == "1":
            return True
        market = self.price(order["symbol"])
        return order["price"] >= market if order["side"] == "1" else order["price"] <= market

    async def _fill(self, order):
        base, rest = divmod(order["qty"], self.fill_chunks)
        chunks = [base] * self.fill_chunks
        chunks[-1] += rest
        for qty in filter(None, chunks):
            await asyncio.sleep(self.fill_delay)
            if order["status"] in FINAL:
                return
            px = self.price(order["symbol"])
            order["cum"] += qty
            order["notional"] += qty * px
            order["status"] = "2" if order["cum"] >= order["qty"] else "1"
            self._report(order, "F", last_qty=qty, last_px=px)

    def _cancel(self, msg):
        order = self.orders.get(msg[41])
        if order is None or order["status"] in FINAL:
            return self.session.send("9", [
                (11, msg[11]), (41, msg[41]), (37, order["order_id"] if order else "NONE"),
                (39, order["status"] if order else "8"), (434, 1), (102, 1 if order is None else 0),
                (58, "unknown order" if order is None else "too late to cancel"),
            ])
        order["status"] = "4"
        self._report(order, "4", clordid=msg[11], extra=[(41, msg[41])])

    def _status(self, msg):
        order = self.orders.get(msg[11])
        if order is None:
            unknown = {"clordid": msg[11], "order_id": "NONE", "symbol": msg.get(55, ""), "side": msg.get(54, "1"),
                       "qty": 0, "cum": 0, "notional": 0.0, "status": "8"}
            return self._report(unknown, "I", text="unknown order")
        self._report(order, "I")

    def _report(self, order, exec_type, last_qty=0, last_px=0.0, clordid=None, extra=(), text=None):
        avg_px = order["notional"] / order["cum"] if order["cum"] else 0.0
        leaves = 0 if order["status"] in FINAL else order["qty"] - order["cum"]
        body = [
            (37, order["order_id"]), (11, clordid or order["clordid"]), (17, uuid.uuid4().hex[:12]),
            (150, exec_type), (39, order["status"]), (55, order["symbol"]), (54, order["side"]),
            (38, order["qty"]), (14, order["cum"]), (151, leaves), (6, f"{avg_px:.4f}"), *extra,
        ]
        if exec_type == "F":
            body += [(32, last_qty), (31, last_px)]
        if text:
            body.append((58, text))
        self.session.send("8", body)
