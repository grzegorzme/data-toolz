import datetime
import json
import pytest


class TestLogging(object):

    logger_name = "test-logger"
    logger_env = "test-env"

    params = [
        {
            "logger-args": {"name": logger_name, "env": logger_env},
            "logger-call": {"msg": "msg-info"},
            "method": "info",
        },
        {
            "logger-args": {"name": logger_name, "env": logger_env},
            "logger-call": {"msg": "msg-error"},
            "method": "error",
        },
        {
            "logger-args": {},
            "logger-call": {"msg": "msg-extra", "qwerty": 123},
            "method": "info",
        },
    ]

    @pytest.mark.parametrize("params", params)
    def test_logger(self, capsys, params):
        import datatoolz.logging as logging

        logger = logging.JsonLogger(**params["logger-args"])

        t_start = datetime.datetime.utcnow().isoformat(sep=" ")
        getattr(logger, params["method"])(**params["logger-call"])
        t_end = datetime.datetime.utcnow().isoformat(sep=" ")

        out, _ = capsys.readouterr()
        log = json.loads(out)

        assert log["level"] == params["method"]
        assert log["message"] == params["logger-call"]["msg"]
        assert t_start <= log["timestamp"] <= t_end
        assert log["logger"]["application"] == params["logger-args"].get("name")
        for k, v in params["logger-call"].items():
            if k != "msg":
                assert log["extra"][k] == v

    def test_logger_decorator(self, capsys):
        import datatoolz.logging as logging

        logger = logging.JsonLogger(name="json-logger", env="test")

        @logger.decorate(
            msg="adding and multiplying",
            static_value="my-value",
            length=lambda x: len(x),
        )
        def my_func(a, b):
            return a + b, a * b

        my_func(42, 2)

        log, _ = capsys.readouterr()
        log = json.loads(log)

        assert log["message"] == "adding and multiplying"
        assert log["level"] == "info"
        assert log["extra"]["function"] == "my_func"
        assert log["extra"]["duration"] >= 0
        assert log["extra"]["memory"]["peak"] >= 0
        assert log["extra"]["static_value"] == "my-value"
        assert log["extra"]["length"] == 2
