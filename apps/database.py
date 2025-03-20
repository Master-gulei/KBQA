# -- coding: utf-8 --
# @Time : 2024/5/22
# @Author : gulei
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config


class Database:
    def __init__(self, ip, port, user, password):
        """
        nebula连接
        :param ip:
        :param port:
        :param user:
        :param password:
        :return:
        """
        self.session = None
        config = Config()
        config.max_connection_pool_size = 20
        config.timeout = 20000
        # init connection pool
        self.connection_pool = ConnectionPool()
        try:
            ok = self.connection_pool.init([(ip, port)], config)
        except:
            raise "nebula init error"
        self.session = self.connection_pool.get_session(user, password)
