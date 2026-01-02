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
