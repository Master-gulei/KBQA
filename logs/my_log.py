# -- coding: utf-8 --
# @Time : 2024/7/22
# @Author : gulei

import logging
from logging.handlers import RotatingFileHandler


class myLogger:
    def __init__(self, loggerName):
        self.logger_obj = logging.getLogger(loggerName)
        self.logger_obj.handlers.clear()

        self.logger_obj.setLevel(logging.DEBUG)

        log_filename = "myLogInfo.txt"
        max_log_size = 1024 * 1024 * 10
        backup_count = 3

        fh = RotatingFileHandler(
            log_filename, maxBytes=max_log_size, backupCount=backup_count
        )  # logging.FileHandler("log/myLogInfo.txt")
        # ch = logging.StreamHandler()

        formater = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(lineno)s - %(message)s"
        )
        fh.setFormatter(formater)
        # ch.setFormatter(formater)

        self.logger_obj.addHandler(fh)
        # self.logger_obj.addHandler(ch)

    def debug(self, message):
        self.logger_obj.debug(message)

    def info(self, message):
        self.logger_obj.info(message)

    def error(self, message):
        self.logger_obj.error(message)

    def warn(self, message):
        self.logger_obj.warn(message)
