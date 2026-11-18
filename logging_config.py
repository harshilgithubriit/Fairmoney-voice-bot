# logging_config.py

import io
import logging
import logging.handlers
import sys
import os
import time
from datetime import datetime

from config.settings import (
    LOGS_DIR,
    LOG_LEVEL,
    LOG_RETENTION_DAYS,
    DISABLE_QUICK_EDIT,
)

LOG_DIR = LOGS_DIR
os.makedirs(LOG_DIR, exist_ok=True)


def disable_quick_edit() -> None:
    """
    Disables QuickEdit mode in Windows command prompt/PowerShell consoles.
    This prevents the console from pausing process execution when clicked.
    """
    if sys.platform != "win32":
        return
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        h_stdin = kernel32.GetStdHandle(-10)  # STD_INPUT_HANDLE = -10
        mode = ctypes.c_ulong()
        if kernel32.GetConsoleMode(h_stdin, ctypes.byref(mode)):
            ENABLE_QUICK_EDIT_MODE = 0x0040
            ENABLE_EXTENDED_FLAGS = 0x0080
            new_mode = (mode.value & ~ENABLE_QUICK_EDIT_MODE) | ENABLE_EXTENDED_FLAGS
            kernel32.SetConsoleMode(h_stdin, new_mode)
    except Exception:
        pass


class DateNamedFileHandler(logging.handlers.TimedRotatingFileHandler):
    """
    Like TimedRotatingFileHandler, but the CURRENTLY OPEN file itself is
    named with today's date (e.g. logs/2026-07-29_voicebot.log), not a
    generic base name. Rolls over to a new date-named file at midnight.
    """

    def __init__(self, log_dir, level=logging.INFO, backupCount=30, encoding="utf-8"):
        self.log_dir = log_dir
        self._current_date = datetime.now().strftime("%Y-%m-%d")
        filename = self._filename_for_date(self._current_date)
        super().__init__(
            filename,
            when="midnight",
            interval=1,
            backupCount=backupCount,
            encoding=encoding,
            utc=False,
        )

    def _filename_for_date(self, date_str: str) -> str:
        return os.path.join(self.log_dir, f"{date_str}_voicebot.log")

    def doRollover(self):
        # Close current stream
        if self.stream:
            self.stream.close()
            self.stream = None

        # Compute new date-named file and point baseFilename at it
        self._current_date = datetime.now().strftime("%Y-%m-%d")
        self.baseFilename = self._filename_for_date(self._current_date)

        if not self.delay:
            self.stream = self._open()

        # Recompute next rollover time (same logic as parent)
        currentTime = int(time.time())
        newRolloverAt = self.computeRollover(currentTime)
        while newRolloverAt <= currentTime:
            newRolloverAt += self.interval
        self.rolloverAt = newRolloverAt


def setup_logging(level=None):
    if DISABLE_QUICK_EDIT:
        disable_quick_edit()

    root = logging.getLogger()
    if root.handlers:
        return

    effective_level = level if level is not None else LOG_LEVEL
    root.setLevel(effective_level)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = DateNamedFileHandler(LOG_DIR, level=effective_level, backupCount=LOG_RETENTION_DAYS)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    console_stream = (
        io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys.stdout, "buffer")
        else sys.stdout
    )
    console_handler = logging.StreamHandler(console_stream)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)

    class StreamToLogger:
        def __init__(self, logger, log_lvl):
            self.logger = logger
            self.level = log_lvl
            self._buffer = ""

        def write(self, message):
            self._buffer += message
            while "\n" in self._buffer:
                line, self._buffer = self._buffer.split("\n", 1)
                if line.strip():
                    self.logger.log(self.level, line)

        def flush(self):
            pass

    sys.stdout = StreamToLogger(logging.getLogger("STDOUT"), logging.INFO)
    sys.stderr = StreamToLogger(logging.getLogger("STDERR"), logging.ERROR)
