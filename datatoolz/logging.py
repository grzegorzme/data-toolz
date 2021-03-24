"""Module allows publishing json-structured log messages"""

import sys
import datetime
import json
import logging
import time
import tracemalloc

INFO = "info"
DEBUG = "debug"
WARNING = "warning"
ERROR = "error"


class JsonLogger:
    """A wrapper on the logging module to produce JSON-structured logs"""

    def __init__(self, name: str = None, env: str = None) -> None:
        self.name = name
        self.env = env
        if name:
            _logger = logging.getLogger(name)
        else:
            _logger = logging.getLogger()

        for handler in _logger.handlers:
            _logger.removeHandler(handler)

        handler = logging.StreamHandler(sys.stdout)
        fmt = "%(message)s"
        handler.setFormatter(logging.Formatter(fmt))
        _logger.addHandler(handler)

        _logger.setLevel(logging.INFO)
        _logger.propagate = False
        self.logger = _logger

    def _log(self, msg: str, level: str, extra=None) -> str:
        assert level in (INFO, WARNING, DEBUG, ERROR)
        j = {
            "logger": {"application": self.name, "environment": self.env},
            "level": level,
            "timestamp": datetime.datetime.utcnow().isoformat(sep=" "),
            "message": msg,
        }
        if extra is not None and len(extra) > 0:
            j["extra"] = extra
        return json.dumps(j)

    def info(self, msg: str, **custom) -> None:
        """
        Log message with INFO level
        :param msg: main log message
        :param custom: any optional data to be added to the log structure
        """
        self.logger.info(msg=self._log(msg=msg, level=INFO, extra=custom))

    def error(self, msg, **custom) -> None:
        """
        Log message with ERROR level
        :param msg: main log message
        :param custom: any optional data to be added to the log structure
        """
        self.logger.error(msg=self._log(msg=msg, level=ERROR, extra=custom))

    def decorate(self, msg: str, duration: bool = True, memory: bool = True, **custom):
        """
        A JsonLogger decorator for logging various execution metrics of a function
        :param msg: static log message
        :param duration: log execution duration
        :param memory: log memory consumption
        :param custom: pass any custom value to log, this can be either a static value
          or a callable executed on the function result
        """

        def inner_decorator(func):
            def wrapper(*args, **kwargs):

                log = {"function": func.__name__}

                if memory:
                    tracemalloc.start()
                start = time.perf_counter()
                result = func(*args, **kwargs)
                end = time.perf_counter()

                if memory:
                    log["memory"] = dict(
                        zip(("current", "peak"), tracemalloc.get_traced_memory())
                    )

                if duration:
                    log["duration"] = end - start

                for name, call_or_value in custom.items():
                    if callable(call_or_value):
                        log[name] = call_or_value(result)
                    else:
                        log[name] = call_or_value
                self.info(msg=msg, **log)
                return result

            return wrapper

        return inner_decorator
