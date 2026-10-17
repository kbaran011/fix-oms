# fix-oms

A FIX 4.4 session engine and order management system in plain Python (asyncio, stdlib only),
with a mock broker to trade against over real TCP.

```
python demo.py        # trading session with a dropped connection mid-fill
python demo.py -v     # same, printing every raw FIX message
python -m unittest discover -s tests -t .
```

## What it does

**Session layer** (`fixoms/session.py`)
- Logon / Logout, CompID validation, first message must be Logon
- Heartbeats; on silence sends TestRequest, then drops the connection if there is still no answer
- Outgoing and incoming sequence numbers persisted to disk (atomic write), so they survive a restart
- Gap detection → ResendRequest; the peer resends application messages with `PossDupFlag=Y` and
  replaces admin messages with SequenceReset-GapFill
- Sequence number too low without PossDup → Logout; garbled/bad-checksum messages are dropped
  without consuming a sequence number
- Messages produced while disconnected keep their sequence numbers and are delivered by resend

**OMS / client** (`fixoms/client.py`)
- NewOrderSingle (market/limit), OrderCancelRequest, handles ExecutionReport and OrderCancelReject
- Order state: PendingNew → New → PartiallyFilled → Filled / Canceled / Rejected, with CumQty/AvgPx
- Fills deduplicated by ExecID (primary key), and stale reports (CumQty going backwards) ignored
- Reconnect with exponential backoff; after every logon, sends OrderStatusRequest for every open order
  and logs mismatches between our records and the broker's
- Positions, average cost, realized and unrealized P&L from the fills table (SQLite)
- Refuses new orders while disconnected instead of queueing them

**Mock broker** (`fixoms/broker.py`): fills marketable orders in chunks (partial fills), rests
non-marketable limit orders, cancels, rejects late cancels and duplicate ClOrdIDs, answers status requests.

## Design decisions

- **Advance `next_in` after handling the message, not before.** If the process dies in between, the
  message gets replayed instead of lost; ExecID dedupe makes the replay harmless.
- **Resend request uses EndSeqNo=0** (everything up to now) and messages above the gap are dropped
  until the resend catches up, which keeps processing strictly in order.
- **No queued orders while offline.** An order sent later at a stale price is worse than a rejection.

## Limitations (it's a learning project)

- No repeating groups, so no market data snapshots (35=W) yet
- The whole sent-message store is rewritten on every send; a real engine would use an append-only log
- SQLite instead of PostgreSQL, single client session, and broker state is held in memory
- Not tested against a real venue yet. The next step is Deribit testnet or another free FIX sandbox
