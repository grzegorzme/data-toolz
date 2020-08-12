"""Module allows publishing json-structured log messages"""

import sys
import datetime
import json
import logging

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

    def info(self, msg: str, **kwargs) -> None:
        """
        Log message with INFO level
        :param msg: main log message
        :param kwargs: any optional data to be added to the log structure
        """
        self.logger.info(msg=self._log(msg=msg, level=INFO, extra=kwargs))

    def error(self, msg, **kwargs) -> None:
        """
        Log message with ERROR level
        :param msg: main log message
        :param kwargs: any optional data to be added to the log structure
        """
        self.logger.error(msg=self._log(msg=msg, level=ERROR, extra=kwargs))
