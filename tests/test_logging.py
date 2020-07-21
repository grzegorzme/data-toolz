import datetime
import json


class TestLogging:

    logger_name = "test-logger"
    logger_env = "test-env"

    def test_logging_info_message(self, capsys):
        import eu_jobs.logging as logging

        logger = logging.JsonLogger(name=self.logger_name, env=self.logger_env)

        msg, t_start = "msg-info", datetime.datetime.utcnow().isoformat(sep=" ")
        logger.info(msg=msg)
        t_end = datetime.datetime.utcnow().isoformat(sep=" ")

        out, err = capsys.readouterr()
        log = json.loads(out)

        assert log["level"] == logging.INFO
        assert log["message"] == msg
        assert t_start <= log["timestamp"] <= t_end
        assert log["logger"]["application"] == self.logger_name

    def test_logging_error_message(self, capsys):
        import eu_jobs.logging as logging

        logger = logging.JsonLogger(name=self.logger_name, env=self.logger_env)

        msg, t_start = "msg-error", datetime.datetime.utcnow().isoformat(sep=" ")
        logger.error(msg=msg)
        t_end = datetime.datetime.utcnow().isoformat(sep=" ")

        out, err = capsys.readouterr()
        log = json.loads(out)
        assert log["level"] == logging.ERROR
        assert log["message"] == msg
        assert t_start <= log["timestamp"] <= t_end
        assert log["logger"]["application"] == self.logger_name

    def test_logging_extended_payload(self, capsys):
        import eu_jobs.logging as logging

        logger = logging.JsonLogger()

        msg, t_start = "msg-extended", datetime.datetime.utcnow().isoformat(sep=" ")
        logger.info(msg=msg, qwerty=123)
        t_end = datetime.datetime.utcnow().isoformat(sep=" ")

        out, err = capsys.readouterr()
        log = json.loads(out)
        assert log["level"] == logging.INFO
        assert log["message"] == msg
        assert t_start <= log["timestamp"] <= t_end
        assert log["extra"]["qwerty"] == 123
        assert log["logger"]["application"] is None
