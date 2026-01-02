from __future__ import annotations
import os
import time
from threading import Lock
MILLISECS_IN_SECS = 1000
NANOSECS_IN_MILLISECS = 1000000
MIN_TIMESTAMP = 0
MAX_TIMESTAMP = 2**48 - 1
MIN_RANDOMNESS = b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
MAX_RANDOMNESS = b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"
TIMESTAMP_LEN = 6
RANDOMNESS_LEN = 10
BYTES_LEN = TIMESTAMP_LEN + RANDOMNESS_LEN
ENCODE = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

def _encode_timestamp(binary: bytes) -> str:
    """Encode 6 bytes of timestamp to 10 base32 characters."""
    lut = ENCODE
    return "".join(
        [
            lut[(binary[0] & 224) >> 5],
            lut[(binary[0] & 31)],
            lut[(binary[1] & 248) >> 3],
            lut[((binary[1] & 7) << 2) | ((binary[2] & 192) >> 6)],
            lut[((binary[2] & 62) >> 1)],
            lut[((binary[2] & 1) << 4) | ((binary[3] & 240) >> 4)],
            lut[((binary[3] & 15) << 1) | ((binary[4] & 128) >> 7)],
            lut[(binary[4] & 124) >> 2],
            lut[((binary[4] & 3) << 3) | ((binary[5] & 224) >> 5)],
            lut[(binary[5] & 31)],
        ]
    )

def _encode_randomness(binary: bytes) -> str:
    """Encode 10 bytes of randomness to 16 base32 characters."""
    lut = ENCODE
    return "".join(
        [
            lut[(binary[0] & 248) >> 3],
            lut[((binary[0] & 7) << 2) | ((binary[1] & 192) >> 6)],
            lut[(binary[1] & 62) >> 1],
            lut[((binary[1] & 1) << 4) | ((binary[2] & 240) >> 4)],
            lut[((binary[2] & 15) << 1) | ((binary[3] & 128) >> 7)],
            lut[(binary[3] & 124) >> 2],
            lut[((binary[3] & 3) << 3) | ((binary[4] & 224) >> 5)],
            lut[(binary[4] & 31)],
            lut[(binary[5] & 248) >> 3],
            lut[((binary[5] & 7) << 2) | ((binary[6] & 192) >> 6)],
            lut[(binary[6] & 62) >> 1],
            lut[((binary[6] & 1) << 4) | ((binary[7] & 240) >> 4)],
            lut[((binary[7] & 15) << 1) | ((binary[8] & 128) >> 7)],
            lut[(binary[8] & 124) >> 2],
            lut[((binary[8] & 3) << 3) | ((binary[9] & 224) >> 5)],
            lut[(binary[9] & 31)],
        ]
    )
