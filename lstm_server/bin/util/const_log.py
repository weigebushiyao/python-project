##
##
##
import os
import  logging
from    logging.handlers import RotatingFileHandler

from    util import const


def init_const():

    const.PROJECT_NAME = "macdlp-lstm-main"

    current_path = os.path.abspath(__file__)
    project_path = os.path.abspath(os.path.dirname(current_path) + os.path.sep + ".." + os.path.sep + "..")
    const.PROJECT_BASEPATH = project_path

    const.LOG_FILE_NAME = project_path + "/logs/macdlp-lstm.log"
    const.JSON_CFG_FILE_NAME = project_path + "/conf/main.service.json.conf"

def init_log(name):
    #logger = logging.getLogger(const.PROJECT_NAME)
    logger = logging.getLogger(name)
    logger.setLevel(level = logging.INFO)

    rHandler = RotatingFileHandler(const.PROJECT_BASEPATH + "/logs/" + name,maxBytes = 1*1024*1024,backupCount = 3)
    rHandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s -%(process)d- %(levelname)s - %(message)s')
    rHandler.setFormatter(formatter)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)

    logger.addHandler(rHandler)
    logger.addHandler(console)

    return logger
