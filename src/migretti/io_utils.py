import os
import tempfile
from contextlib import contextmanager
from typing import Generator, TextIO, cast

@contextmanager
def atomic_write(filepath: str, mode: str = "w", encoding: str = "utf-8", exclusive: bool = False) -> Generator[TextIO, None, None]:
    """
    Open a file for atomic writing.
    
    1. Creates a temporary file in the same directory.
    2. Yields the file object for writing.
    3. On success: fsyncs, closes, and renames the temp file to the target (atomic).
    4. On failure: deletes the temp file.
    
    Args:
        filepath: Target file path.
        mode: File open mode (default 'w').
        encoding: File encoding (default 'utf-8').
        exclusive: If True, fails if target file already exists (simulates 'x' mode).
    """
    if exclusive and os.path.exists(filepath):
        raise FileExistsError(f"File '{filepath}' already exists.")
        
    dir_name = os.path.dirname(os.path.abspath(filepath))
    prefix = os.path.basename(filepath) + "."
    
    # Create temp file in same directory to ensure atomic rename works (same filesystem)
    fd, temp_path = tempfile.mkstemp(prefix=prefix, dir=dir_name, text=True)
    
    try:
        with os.fdopen(fd, mode, encoding=encoding) as f:
            yield cast(TextIO, f)
            f.flush()
            os.fsync(f.fileno())
            
        # Atomic rename
        os.replace(temp_path, filepath)
    except Exception:
        # Cleanup on error
        if os.path.exists(temp_path):
            os.remove(temp_path)
        raise