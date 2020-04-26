import os
import signal
import time
from multiprocessing import Process,Queue,Manager
#from client_process import client_process

sub_prosess = []
subinfo_queue = Queue()


from   util import const
from   util.const_log import init_const
from   util.const_log import init_log
from   conf_process.config_josn import main_service_json_conf
from   my_process.main_process import main_process
import fcntl,sys,os
pidfile=0
pidfile = open(os.path.realpath(__file__), "r")
try:
    fcntl.flock(pidfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    #创建一个排他锁,并且所被锁住其他进程不会阻塞
except:
    print("another instance is running...")
    sys.exit(1)

## init ###################
init_const()
logger = init_log(const.PROJECT_NAME + '.base')

logger.info("=====================================================")
logger.info("Start access log")

logger.info("read configure.")
## init configure and cheack it`s change.
main_cfg = main_service_json_conf(const.JSON_CFG_FILE_NAME)

import sys
print (sys.path)

main_process_run_flg = True

def signal_handler(signum, frame):
    for cur_p in sub_prosess:
        cur_p.terminate()

    global main_process_run_flg
    main_process_run_flg = False

##set signal handler
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

logger.info("create process number:" + str(main_cfg.get_value("work_process")))
for i in range(main_cfg.get_value("work_process")):
    p= main_process(subinfo_queue)
    p.daemon = True
    p.start()
    sub_prosess.append(p)

logger.info("main loop starting...")
## main loop
while main_process_run_flg:

    while not subinfo_queue.empty():
        msg = subinfo_queue.get_nowait()
        logger.info("sub process status:")
        logger.info(msg)

    #logger.info("sub process check..")
    for cur_p in sub_prosess:
        #check
        if not cur_p.is_alive():
            logger.error("vmain process is exited. exitcode: %d " %cur_p.exitcode )
            sub_prosess.remove(cur_p)

            new_p = main_process(subinfo_queue)
            new_p.daemon = True
            new_p.start()
            sub_prosess.append(new_p)

            logger.error("main process is restart.")

    time.sleep(5)

logger.info("main process stoped.")
exit(0)
