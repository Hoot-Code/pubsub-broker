"""Unit tests for the pubsub wire protocol (FIX 1)."""
import struct
import unittest

from pubsub.protocol import (
    HEADER_SIZE,
    MAGIC,
    VERSION,
    _HEADER_FMT,
    CMD_AUTH,
    CMD_PING,
    CMD_PUBLISH,
    CMD_RESPONSE,
    CMD_ERROR,
    CMD_PUSH,
    CMD_SEEK,
    CMD_RESET_GROUP,
    CMD_COMMIT_OFFSET,
    encode_frame,
    decode_frame,
)


class TestEncodeDecodeRoundTrip(unittest.TestCase):
    """encode_frame → decode_frame must preserve all fields."""

    def _round_trip(self, cmd, req_id, body):
        frame = encode_frame(cmd, req_id, body)
        version, got_cmd, got_req_id, got_body_bytes = decode_frame(frame)
        return version, got_cmd, got_req_id, got_body_bytes

    def test_publish_with_body(self):
        body = {"topic": "events", "payload": "aGVsbG8=", "delivery_mode": 0}
        req_id = 42
        version, cmd, got_id, body_bytes = self._round_trip(
            CMD_PUBLISH, req_id, body
        )
        import json
        self.assertEqual(version, VERSION)
        self.assertEqual(cmd, CMD_PUBLISH)
        self.assertEqual(got_id, req_id)
        decoded = json.loads(body_bytes.decode("utf-8"))
        self.assertEqual(decoded["topic"], "events")

    def test_empty_body(self):
        req_id = 0
        version, cmd, got_id, body_bytes = self._round_trip(CMD_AUTH, req_id, None)
        self.assertEqual(version, VERSION)
        self.assertEqual(cmd, CMD_AUTH)
        self.assertEqual(got_id, 0)
        self.assertEqual(body_bytes, b"")

    def test_all_fields_preserved(self):
        req_id = 0xDEADBEEFCAFEBABE
        body = {"key": "value", "num": 123}
        version, cmd, got_id, body_bytes = self._round_trip(
            CMD_RESPONSE, req_id, body
        )
        import json
        self.assertEqual(version, VERSION)
        self.assertEqual(cmd, CMD_RESPONSE)
        self.assertEqual(got_id, req_id)
        parsed = json.loads(body_bytes)
        self.assertEqual(parsed["num"], 123)

    def test_unicode_body(self):
        body = {"msg": "héllo wörld 🎉"}
        frame = encode_frame(CMD_PUBLISH, 1, body)
        _v, cmd, _id, body_bytes = decode_frame(frame)
        import json
        self.assertEqual(cmd, CMD_PUBLISH)
        decoded = json.loads(body_bytes.decode("utf-8"))
        self.assertEqual(decoded["msg"], "héllo wörld 🎉")


class TestMagicBytes(unittest.TestCase):
    """encode_frame must embed MAGIC = 0x50534201 in the first 4 bytes LE."""

    def test_magic_in_first_four_bytes(self):
        frame = encode_frame(CMD_PUBLISH, 1, {"topic": "t"})
        extracted_magic = struct.unpack("<I", frame[0:4])[0]
        self.assertEqual(extracted_magic, 0x50534201)

    def test_magic_matches_constant(self):
        frame = encode_frame(CMD_AUTH, 99, {"api_key": "x"})
        extracted_magic = struct.unpack("<I", frame[0:4])[0]
        self.assertEqual(extracted_magic, MAGIC)

    def test_version_byte(self):
        frame = encode_frame(CMD_PING, 0, None)
        # Byte index 4 is the version field.
        self.assertEqual(frame[4], VERSION)
        self.assertEqual(frame[4], 0x01)

    def test_cmd_byte(self):
        frame = encode_frame(CMD_PUSH, 0, None)
        # Byte index 5 is the command field.
        self.assertEqual(frame[5], CMD_PUSH)
        self.assertEqual(frame[5], 0x20)

    def test_push_is_0x20(self):
        """CMD_PUSH must match Go CmdPush = 0x20."""
        self.assertEqual(CMD_PUSH, 0x20)

    def test_seek_is_0x30(self):
        self.assertEqual(CMD_SEEK, 0x30)

    def test_reset_group_is_0x31(self):
        self.assertEqual(CMD_RESET_GROUP, 0x31)

    def test_commit_offset_is_0x07(self):
        self.assertEqual(CMD_COMMIT_OFFSET, 0x07)


class TestHeaderSize(unittest.TestCase):
    """encode_frame must produce exactly HEADER_SIZE + len(body) bytes."""

    def test_empty_body_is_header_only(self):
        frame = encode_frame(CMD_PING, 0, None)
        self.assertEqual(len(frame), HEADER_SIZE)
        self.assertEqual(HEADER_SIZE, 18)

    def test_body_length_appended(self):
        import json
        body = {"topic": "orders"}
        body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
        frame = encode_frame(CMD_PUBLISH, 1, body)
        self.assertEqual(len(frame), HEADER_SIZE + len(body_bytes))

    def test_body_len_field_matches_actual(self):
        body = {"a": 1, "b": "two"}
        frame = encode_frame(CMD_RESPONSE, 7, body)
        _magic, _ver, _cmd, _req_id, body_len = struct.unpack(
            _HEADER_FMT, frame[:HEADER_SIZE]
        )
        self.assertEqual(body_len, len(frame) - HEADER_SIZE)

    def test_struct_size_is_18(self):
        """Sanity: <IBBQI must be exactly 18 bytes."""
        self.assertEqual(struct.calcsize(_HEADER_FMT), 18)

    def test_large_body(self):
        body = {"data": "x" * 65536}
        frame = encode_frame(CMD_PUBLISH, 999, body)
        self.assertGreater(len(frame), HEADER_SIZE)
        _v, cmd, _id, body_bytes = decode_frame(frame)
        self.assertEqual(cmd, CMD_PUBLISH)
        self.assertEqual(len(body_bytes) + HEADER_SIZE, len(frame))


class TestInvalidFrames(unittest.TestCase):
    """decode_frame must reject malformed input."""

    def test_wrong_magic(self):
        bad = struct.pack(_HEADER_FMT, 0xDEADBEEF, 0x01, 0x01, 1, 0)
        with self.assertRaises(ValueError):
            decode_frame(bad)

    def test_too_short(self):
        with self.assertRaises(ValueError):
            decode_frame(b"\x01\x42\x53\x50")  # only 4 bytes


if __name__ == "__main__":
    unittest.main()
