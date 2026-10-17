import unittest

from fixoms import fix


class FixCodecTest(unittest.TestCase):
    def test_round_trip(self):
        raw = fix.encode("D", "CLIENT", "BROKER", 7, [(11, "abc"), (38, 100)])
        msg = fix.decode(raw)
        self.assertEqual(msg[35], "D")
        self.assertEqual(msg[34], "7")
        self.assertEqual(msg[11], "abc")
        self.assertEqual(msg[38], "100")

    def test_bad_checksum_rejected(self):
        raw = fix.encode("0", "A", "B", 1)
        tampered = raw.replace(b"49=A", b"49=X")
        with self.assertRaises(fix.FixError):
            fix.decode(tampered)

    def test_split_handles_partial_and_multiple_messages(self):
        a = fix.encode("0", "A", "B", 1)
        b = fix.encode("0", "A", "B", 2)
        msgs, rest = fix.split_messages(a + b[:10])
        self.assertEqual(msgs, [a])
        msgs, rest = fix.split_messages(rest + b[10:])
        self.assertEqual(msgs, [b])
        self.assertEqual(rest, b"")

    def test_split_skips_garbage(self):
        a = fix.encode("0", "A", "B", 1)
        msgs, _ = fix.split_messages(b"junk\x01" + a)
        self.assertEqual(msgs, [a])


if __name__ == "__main__":
    unittest.main()
