import csv
import time
import os
import signal
import sys
import threading

class BufferedLogger:
    def __init__(self, file_path, fieldnames, buffer_size=100, flush_interval=10):
        self.file_path = file_path
        self.fieldnames = fieldnames
        self.buffer = []
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.last_flush_time = time.time()
        self.lock = threading.Lock()
        self.file_initialized = os.path.exists(self.file_path)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
    def log(self, data_dict):
        with self.lock:
            self.buffer.append(data_dict)
            should_flush = (
                len(self.buffer) >= self.buffer_size or
                (time.time() - self.last_flush_time) > self.flush_interval
            )
            if should_flush:
                self.flush()
    def flush(self):
        if not self.buffer:
            return
        try:
            with open(self.file_path, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.fieldnames)
                if os.stat(self.file_path).st_size == 0:
                    writer.writeheader()
                writer.writerows(self.buffer)
                f.flush()
                os.fsync(f.fileno())
            self.last_flush_time = time.time()
            self.buffer = []
        except Exception as e:
            print(f"ERROR: {e}", flush=True)
    def close(self):
        with self.lock:
            self.flush()
    def _signal_handler(self, signum, frame):
        self.close()
        sys.exit(0)
