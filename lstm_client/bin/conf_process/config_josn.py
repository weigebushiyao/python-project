import os
import time
import threading
import json
from hashlib import md5

import logging
from util import const

class client_service_json_conf():

    conf_file_name = ""
    conf_file_md5=""

    ## null conf
    conf_json = json.loads('{}')

    def init_default(self):
        self.conf_json["work_process"]      = 2

        self.conf_json["mysql_host"]        = "192.168.6.131"
        self.conf_json["mysql_port"]        = "3306"
        self.conf_json["mysql_user"]        = "root"
        self.conf_json["mysql_passwd"]      = "esn-mac-dlp"
        self.conf_json["mysql_db_video"]    = "macdlp_django"

        self.conf_json["es_host"]            = "192.168.6.33"
        self.conf_json["es_port"]            = "9200"

        self.conf_json["local_uid_path"]    = const.PROJECT_BASEPATH + "/macdlp/client/local_uid/"
        self.conf_json["train_data"]        = const.PROJECT_BASEPATH + "/macdlp/client/train_data/"
        self.conf_json["valid_data"]        = const.PROJECT_BASEPATH + "/macdlp/client/valid_data/"
        self.conf_json["modle_path"]        = const.PROJECT_BASEPATH + "/macdlp/client/modle_path/"

        self.conf_json["lstm_input_size"]   = 219
        self.conf_json["data_extract_len"]  = 20
        self.conf_json["lstm_rnn_unit"]     = 256
        self.conf_json["lstm_time_steps"]   = 10
        self.conf_json["lstm_batch_size"]   = 10
        self.conf_json["lstm_lr"]           = 0.001
        self.conf_json["lstm_output"] = 40000
        self.conf_json["lstm_learn_days"]   = 30
        self.conf_json["lstm_learn_min_data"]= 80
        self.conf_json["lstm_max_learn_cnt"]= 80
        self.conf_json["incr_learn_min_count"] = 5
        self.conf_json["lstm_end_loss"]     = 0.001
        self.conf_json["lstm_min_predict_cnt"] = 20
        self.conf_json["wait_idle"]         = 1
        self.conf_json["check_mode"]        = 1
        self.conf_json["train_epo"] = 1
        self.conf_json["check_min_acc"] = 1


    def __init__(self,fn):
        self.logger = logging.getLogger(const.PROJECT_NAME+'.base')

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

            self.conf_json["local_uid_path"] = const.PROJECT_BASEPATH + conf_txt["local_uid_path"]
            self.conf_json["train_data"] = const.PROJECT_BASEPATH + conf_txt["train_data"]
            self.conf_json["valid_data"] = const.PROJECT_BASEPATH + conf_txt["valid_data"]
            self.conf_json["predict_data"] = const.PROJECT_BASEPATH + conf_txt["predict_data"]
            self.conf_json["modle_path"] = const.PROJECT_BASEPATH + conf_txt["modle_path"]

            #for client_conf in  self.conf_json["clients"] :
            #    client_conf["client_uid_path"] = const.PROJECT_BASEPATH + client_conf["client_uid_path"];

            self.logger.info("configure file if loaded.")
            self.logger.info('--configure-----------------------------------------------')
            self.logger.info(json.dumps(self.conf_json, indent=2))

        except:
            self.logger.error('configure file is not exists, or invalid. use the default value.')
            self.logger.error('--configure default---------------------------------------')
            self.logger.error(json.dumps(self.conf_json, indent=2))


    def get_value(self,key):
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

        while(1):
            time.sleep(60)

            self.logger.info("check configure file is changed...")
            cur_md5 = self.calc_conf_file_md5()

            if( self.conf_file_md5 != cur_md5):
                self.conf_file_md5 = cur_md5
                self.load_conf_data()

