from __future__ import annotations
import os
import time
from threading import Lock
MILLISECS_IN_SECS = 1000
NANOSECS_IN_MILLISECS = 1000000
MIN_TIMESTAMP = 0
MAX_TIMESTAMP = 2**48 - 1
