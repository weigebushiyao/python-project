import os
import time
import threading
import json
from hashlib import md5

import logging
from util import const


class main_service_json_conf():
    conf_file_name = ""
    conf_file_md5 = ""
    ## null conf
    conf_json = json.loads('{}')

    def init_default(self):
        self.conf_json["work_process"] = 1

        self.conf_json["mysql_host"] = "tis_db"
        self.conf_json["mysql_port"] = "3306"
        self.conf_json["mysql_user"] = "root"
        self.conf_json["mysql_passwd"] = "esn-mac-dlp"
        self.conf_json["mysql_db_video"] = "macdlp_django"

        self.conf_json["es_host"] = "es_es"
        self.conf_json["es_host"] = "9200"
        self.conf_json["local_uid_path"] = const.PROJECT_BASEPATH + "/macdlp/main/local_uid"
        self.conf_json["client_count"] = 2

    def __init__(self, fn):
        self.logger = logging.getLogger(const.PROJECT_NAME + '.base')

        self.logger.info("init configure file.")
        self.conf_file_name = fn
        self.init_default()

        self.conf_file_md5 = self.calc_conf_file_md5()
        self.load_conf_data()

        self.logger.info("start thread to check configure file changed.")
        t = threading.Thread(target=self.check_change)
        t.setDaemon(True)
        t.start()


    def load_conf_data(self):
        self.logger.info("load access param from configure file.")
        try:
            fo = open(self.conf_file_name)
            conf_txt = fo.read()
            self.conf_json = json.loads(conf_txt)

            self.conf_json["local_uid_path"] = const.PROJECT_BASEPATH + self.conf_json["local_uid_path"]
            # for client_conf in  self.conf_json["clients"] :
            #    client_conf["client_uid_path"] = const.PROJECT_BASEPATH + client_conf["client_uid_path"];

            self.logger.info("configure file if loaded.")
            self.logger.info('--configure-----------------------------------------------')
            self.logger.info(json.dumps(self.conf_json, indent=2))

        except:
            self.logger.error('configure file is not exists, or invalid. use the default value.')
            self.logger.error('--configure default---------------------------------------')
            self.logger.error(json.dumps(self.conf_json, indent=2))


    def get_value(self, key):
        val = None
        try:
            val = self.conf_json[key]
        except:
            self.conf_json[key] = val

        return val


    def calc_conf_file_md5(self):
        m = md5()
        f = open(self.conf_file_name, 'rb')
        m.update(f.read())
        f.close()
        return m.hexdigest()


    def check_change(self):
        while (1):
            time.sleep(60)
            self.logger.info("check configure file is changed...")
            cur_md5 = self.calc_conf_file_md5()
            if (self.conf_file_md5 != cur_md5):
                self.conf_file_md5 = cur_md5
                self.load_conf_data()
