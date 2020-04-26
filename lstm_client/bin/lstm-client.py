
import signal
import time
from ctypes import c_char_p
from multiprocessing import Queue,Value,Array
from threading import Thread


from   util import const
from   util.const_log import init_const
from   util.const_log import init_log
from   conf_process.config_josn import client_service_json_conf
from   my_process.client_process import client_process
from   my_process.predict_process import predict_process
import my_process.global_obj as global_obj
from my_process.manager_uid import thread_manager_uid_link,thread_manager_procee
import fcntl,sys,os
pidfile=0
pidfile = open(os.path.realpath(__file__), "r")
try:
    fcntl.flock(pidfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    #创建一个排他锁,并且所被锁住其他进程不会阻塞
except:
    print("another instance is running...")
    sys.exit(1)

subinfo_queue = Queue()

## init ###################
init_const()
global_obj.main_logger = init_log(const.PROJECT_NAME + '.base')

global_obj.main_logger.info("=====================================================")
global_obj.main_logger.info("Start access log")

global_obj.main_logger.info("read configure.")
## init configure and cheack it`s change.
global_obj.main_cfg = client_service_json_conf(const.JSON_CFG_FILE_NAME)

# 初始化数据库
global_obj.mydbspool = global_obj.init_mysql()

#
main_process_run_flg = True
def signal_handler(signum, frame):
    for cur_p in global_obj.sub_prosess.keys():
        cur_p.terminate()

    global main_process_run_flg
    main_process_run_flg = False

##set signal handler
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


#####################################################################################################
# 主进程
#    1.监视子进程运行，子进程异常时，重新启动子进程
#    2.通过Quere，接收各子进程的统计信息
#    3.主进程开启2个子线程，通过共享有效uid队列(access_uid_link)与uid信息表(access_uid_dict),完成分配各子进程工作
#        子线程1：从数据库lstm管理表，追加新uid信息，设置需停止信息
#        子线程2：以共享有效uid队列为基础，启动停止与分配uid到各工作子进程工作
#
#    共享有效uid队列：有序UID列表，决定UID的计算顺序
#    uid信息表：保存UID信息（计算使用子进程，学习完成移除标记，再学习启动标记

# 子进程启动
global_obj.main_logger.info("create process num:" + str(global_obj.main_cfg.get_value("work_process")))
for i in range(global_obj.main_cfg.get_value("work_process")):
    run_status = Value('i',0)
    #active_uid = Value(c_char_p, b'abcdefg')
    active_uid = Array('c',256)
    p= client_process(subinfo_queue,run_status,active_uid)
    p.daemon = True
    p.start()
    global_obj.sub_prosess[p]={'run_status':run_status , 'active_uid' : active_uid}
pp= predict_process(subinfo_queue)
pp.daemon = True
pp.start()
# 管理线程启动
thr_mg_uid = Thread(target=thread_manager_uid_link)
thr_mg_uid.setDaemon(True)
thr_mg_uid.start()
thr_mg_pro = Thread(target=thread_manager_procee)
thr_mg_pro.setDaemon(True)
thr_mg_pro.start()
## main loop
global_obj.main_logger.info("main lopp start...")
while main_process_run_flg:
    while not subinfo_queue.empty():
        msg = subinfo_queue.get_nowait()
        global_obj.main_logger.info("sub process status:")
        global_obj.main_logger.info(msg)

    global_obj.main_logger.info("sub process check..")
    for cur_p in global_obj.sub_prosess.keys():
        #check當前uid死掉則移除隊列
        if not cur_p.is_alive():
            global_obj.main_logger.error("vmain process is exited. exitcode: %d " %cur_p.exitcode )
            global_obj.sub_prosess_lock.acquire()
            global_obj.sub_prosess.pop(cur_p)
            global_obj.sub_prosess_lock.release()

            run_status = Value('i', 0)
            #active_uid = Value(c_char_p, b'')
            active_uid = Array('c', 256)
            new_p = client_process(subinfo_queue,run_status,active_uid)
            new_p.daemon = True
            new_p.start()

            global_obj.sub_prosess_lock.acquire()
            global_obj.sub_prosess[new_p] = {'run_status':run_status , 'active_uid' : active_uid}
            global_obj.sub_prosess_lock.release()

            global_obj.main_logger.error("main process is restart.")

    time.sleep(5)
# 线程回收
global_obj.thr_running_flg = False
thr_mg_uid.join()
thr_mg_pro.join()

global_obj.main_logger.info("main process stoped.")
exit(0)
