"""FIX session layer: logon, heartbeats, sequence numbers, gap detection and resend."""
import asyncio
import json
import logging
import os
import time
from collections import deque

from . import fix

ADMIN_TYPES = {"0", "1", "2", "3", "4", "5", "A"}


class Session:
    def __init__(self, sender, target, store_path, on_app=None, on_logon=None, heartbeat=30):
        self.sender = sender
        self.target = target
        self.store_path = store_path
        self.on_app = on_app
        self.on_logon = on_logon
        self.heartbeat = heartbeat
        self.log = logging.getLogger(f"session.{sender}")
        self.history = deque(maxlen=2000)  # (direction, msg dict), for debugging and tests
        self.writer = None
        self.logged_in = False
        self._load()

    # --- persistence -------------------------------------------------------

    def _load(self):
        if os.path.exists(self.store_path):
            with open(self.store_path) as f:
                state = json.load(f)
            self.next_out = state["next_out"]
            self.next_in = state["next_in"]
            self.sent = state["sent"]  # {str(seq): [msg_type, body, sending_time]}
        else:
            self.next_out, self.next_in, self.sent = 1, 1, {}

    def _save(self):
        tmp = self.store_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"next_out": self.next_out, "next_in": self.next_in, "sent": self.sent}, f)
        os.replace(tmp, self.store_path)  # atomic, so a crash never leaves a half-written store

    # --- sending -----------------------------------------------------------

    @property
    def connected(self):
        return self.writer is not None and not self.writer.is_closing()

    def send(self, msg_type, body=()):
        """Assign the next sequence number, store the message, and write it if connected.

        Messages sent while disconnected keep their sequence number and reach the peer
        through its ResendRequest after it reconnects."""
        seq = self.next_out
        self.next_out += 1
        now = fix.utc_now()
        body = [[tag, str(value)] for tag, value in body]
        self.sent[str(seq)] = [msg_type, body, now]
        self._save()
        if self.connected:
            self._write(fix.encode(msg_type, self.sender, self.target, seq, body, sending_time=now))
        return seq

    def _write(self, raw):
        self.log.debug("OUT %s", fix.pretty(raw))
        self.history.append(("out", fix.decode(raw)))
        self.writer.write(raw)
        self.last_sent = time.monotonic()

    def disconnect(self):
        if self.writer is not None and not self.writer.is_closing():
            self.writer.close()

    # --- connection loop ---------------------------------------------------

    async def run(self, reader, writer, initiate=False):
        self.writer = writer
        self.initiator = initiate
        self.logged_in = False
        self.resend_until = None
        self.test_req_id = None
        self.last_recv = self.last_sent = time.monotonic()
        if initiate:
            self.send("A", [(98, 0), (108, self.heartbeat)])
        heartbeats = asyncio.create_task(self._heartbeat_loop())
        buf = b""
        try:
            while self.connected:
                data = await reader.read(65536)
                if not data:
                    break
                self.last_recv = time.monotonic()
                self.test_req_id = None
                msgs, buf = fix.split_messages(buf + data)
                for raw in msgs:
                    self._on_raw(raw)
        except ConnectionError as e:
            self.log.warning("connection error: %s", e)
        finally:
            heartbeats.cancel()
            self.logged_in = False
            self.disconnect()
            self.writer = None
            self.log.info("disconnected (next_out=%d next_in=%d)", self.next_out, self.next_in)

    async def _heartbeat_loop(self):
        while self.connected:
            await asyncio.sleep(self.heartbeat / 4)
            if not self.logged_in:
                continue
            now = time.monotonic()
            if now - self.last_sent >= self.heartbeat:
                self.send("0")
            silence = now - self.last_recv
            if silence > self.heartbeat * 2.4:
                self.log.warning("no response to TestRequest, dropping connection")
                self.disconnect()
            elif silence > self.heartbeat * 1.2 and self.test_req_id is None:
                self.test_req_id = f"TEST-{int(time.time() * 1000)}"
                self.send("1", [(112, self.test_req_id)])

    # --- receiving ---------------------------------------------------------

    def _on_raw(self, raw):
        try:
            msg = fix.decode(raw)
        except fix.FixError as e:
            self.log.warning("dropping garbled message (%s): %s", e, fix.pretty(raw))
            return  # garbled messages are ignored and do not consume a sequence number
        self.log.debug("IN  %s", fix.pretty(raw))
        self.history.append(("in", msg))

        msg_type, seq = msg.get(35), int(msg.get(34, 0))
        if msg.get(49) != self.target or msg.get(56) != self.sender:
            self.log.error("wrong CompIDs %s->%s, disconnecting", msg.get(49), msg.get(56))
            return self.disconnect()
        if not self.logged_in and msg_type != "A":
            self.log.error("first message was not Logon, disconnecting")
            return self.disconnect()
        if msg_type == "4" and msg.get(123) != "Y":
            self.next_in = int(msg[36])  # SequenceReset-Reset ignores sequence numbers
            return self._save()

        if seq < self.next_in:
            if msg.get(43) == "Y":
                return  # resent duplicate we already processed
            self.send("5", [(58, f"MsgSeqNum too low, expected {self.next_in} received {seq}")])
            return self.disconnect()

        if seq > self.next_in:
            if msg_type == "A":
                self._on_admin(msg)  # accept the logon, then recover the gap
            if self.resend_until is None:
                self.log.warning("sequence gap: expected %d received %d, requesting resend", self.next_in, seq)
                self.resend_until = seq
                self.send("2", [(7, self.next_in), (16, 0)])
            return  # dropped here, it will come back in the resend

        if msg_type in ADMIN_TYPES:
            self._on_admin(msg)
        elif self.on_app:
            try:
                self.on_app(msg)
            except Exception:
                self.log.exception("application handler failed for seq %d", seq)
        # Advance after handling: a crash in between means the message is replayed,
        # which is why the application dedupes fills by ExecID.
        self.next_in = int(msg[36]) if msg_type == "4" else self.next_in + 1
        if self.resend_until is not None and self.next_in > self.resend_until:
            self.log.info("gap recovered, next_in=%d", self.next_in)
            self.resend_until = None
        self._save()

    def _on_admin(self, msg):
        msg_type = msg[35]
        if msg_type == "A" and not self.logged_in:
            self.logged_in = True
            if not self.initiator:
                self.send("A", [(98, 0), (108, self.heartbeat)])
            self.log.info("logged on")
            if self.on_logon:
                self.on_logon()
        elif msg_type == "1":
            self.send("0", [(112, msg.get(112, ""))])
        elif msg_type == "2":
            self._resend(int(msg[7]), int(msg.get(16, 0)))
        elif msg_type == "3":
            self.log.warning("session reject of seq %s: %s", msg.get(45), msg.get(58))
        elif msg_type == "5":
            self.log.info("peer logged out: %s", msg.get(58, ""))
            self.disconnect()

    def _resend(self, begin, end):
        """Resend application messages with PossDupFlag=Y; replace admin messages with GapFills."""
        last = self.next_out - 1 if end == 0 else min(end, self.next_out - 1)
        self.log.info("resending %d..%d", begin, last)
        gap_start = None
        for seq in range(begin, last + 1):
            stored = self.sent.get(str(seq))
            if stored is None or stored[0] in ADMIN_TYPES:
                gap_start = seq if gap_start is None else gap_start
                continue
            if gap_start is not None:
                self._gap_fill(gap_start, seq)
                gap_start = None
            msg_type, body, orig_time = stored
            self._write(fix.encode(msg_type, self.sender, self.target, seq, body,
                                   poss_dup=True, orig_time=orig_time))
        if gap_start is not None:
            self._gap_fill(gap_start, last + 1)

    def _gap_fill(self, seq, new_seq):
        self._write(fix.encode("4", self.sender, self.target, seq, [(123, "Y"), (36, new_seq)], poss_dup=True))
