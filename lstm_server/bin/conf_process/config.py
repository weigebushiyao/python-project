import configparser
import time
import threading
from hashlib import md5

import logging
from util import const

class video_access_conf():

    conf_file_name = ""
    conf_file_md5=""

    #default param
    work_process = 4
    database_url="192.168.6.131:3306"

    es_url="192.168.6.33:9200"

    video_data_path = "/data/v_data"
    picture_data_path = "/data/p_data"


    def __init__(self,fn):
        self.logger = logging.getLogger(const.PROJECT_NAME)
        self.logger.info("init configure file.")
        self.conf_file_name = fn

        self.conf_file_md5 = self.calc_conf_file_md5()
        self.load_conf_data()

        self.logger.info("start thread to check configure file changed.")
        t = threading.Thread(target=self.check_change)
        t.setDaemon(True)
        t.start()


    def load_conf_data(self):
        self.logger.info("load access param from configure file.")

        self.parser = configparser.ConfigParser()
        self.parser.read(self.conf_file_name)

        self.work_process = self.parser.get(const.PROJECT_NAME,"work_process")
        self.database_url = self.parser.get(const.PROJECT_NAME,"database_url")
        self.hbase_url = self.parser.get(const.PROJECT_NAME,"hbase_url")
        self.es_url = self.parser.get(const.PROJECT_NAME,"es_url")

        self.video_data_path = const.PROJECT_BASEPATH + "/" + self.parser.get(const.PROJECT_NAME,"video_data_path")
        self.picture_data_path = const.PROJECT_BASEPATH + "/" + self.parser.get(const.PROJECT_NAME,"pictrue_data_path")

    def calc_conf_file_md5(self):
        m = md5()
        f = open(self.conf_file_name, 'rb')
        m.update(f.read())
        f.close()

        return m.hexdigest()

    def check_change(self):

        while(1):
            time.sleep(60)

            self.logger.info("check configure file is cheanged?")
            cur_md5 = self.calc_conf_file_md5()

            if( self.conf_file_md5 != cur_md5):
                self.conf_file_md5 = cur_md5
                self.load_conf_data()

