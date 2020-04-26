from typing import Dict, Any

import  os
from threading import Lock
import  socket

main_logger = None
main_cfg = None
#local_host_name = os.getenv('HOSTNAME')
local_host_name=socket.gethostname()
print('local host name',local_host_name)
#for test
#local_host_name = 'esn-doc-ds-dt-1'

access_uid_link = list()
access_uid_dict = dict()
sub_prosess: Dict[Any, Any] = dict()

access_uid_link_lock = Lock()
access_uid_dict_lock = Lock()
sub_prosess_lock = Lock()

# 线程回收用
thr_running_flg = True

###########################################################

#进程状态run_status：
# 0: 空闲      主进程初始化或进程检测时设置
# 1: 等待开启  启动uid学习时，进程管理线程设置
# 2: 运行中    算法子进程开始学习工作时设置
# 3: 等待结束  算法子进程学习完成时设置，此时可被回收，再次使用
# -1:异常      子进程异常，不能工作
def get_idle_process():
    #print("start get_idle_process function")
    try:
        #进程回收
        #print("start first for loop")
        for p in sub_prosess.keys():
            #main_logger.info("Check idel process:[" + p.name + "]")
            if sub_prosess[p]['run_status'].value == 3:
                # 清除uid信息中的运行标记
                access_uid_dict_lock.acquire()
                access_uid_dict[p.active_uid.value.decode('utf-8')]['is_running'] = 0
                access_uid_dict_lock.release()
                # 状态设置为空闲
                sub_prosess_lock.acquire()
                sub_prosess[p]['run_status'].value = 0
                sub_prosess[p]['active_uid'].value = b''
                sub_prosess_lock.release()

                set_running_state_to_dict(p.active_uid.value.decode('utf-8'),0)

                main_logger.info("Set idel process:[" + p.name + "] uid[" + p.active_uid.value.decode('utf-8') + "]")
        #进程分配
        #print("start second for loop")
        for p in sub_prosess.keys():
            if sub_prosess[p]['run_status'].value == 0:
                main_logger.info("Idel process finded. [" + p.name + "]")
                return p

        main_logger.info("Idel process not found.")
        #print("Idel process not found.")
    except:
        main_logger.error("check idle process error.")
    #print("start return None")
    return None

def start_process(process,cur_uid):
    sub_prosess_lock.acquire()
    #sub_prosess[process]['active_uid'].value = cur_uid.encode('utf-8')
    sub_prosess[process]['active_uid'].value = cur_uid.encode('utf-8')
    sub_prosess[process]['run_status'].value = 1
    sub_prosess_lock.release()
    main_logger.info("Start process. [" + process.name + "] uid[" + cur_uid + "]")

###########################################################

def get_uid_link():
    return access_uid_link

def get_uid_from_list(uid):
    idx = -1
    try:
        idx = access_uid_link.index(uid)
    except:
        idx = -1
    return idx

def add_uid_to_list(uid):
    main_logger.info("run add_uid_to_list() .uid[" + uid + "]")
    # 不存在时添加到队列尾
    if get_uid_from_list(uid) == -1:
        access_uid_link_lock.acquire()
        access_uid_link.append(uid)
        access_uid_link_lock.release()
    else:
        main_logger.info("Not add uid to list. uid[" + uid + "]")

def del_uid_from_list(uid):
    main_logger.info("run del_uid_to_list() .uid[" + uid + "]")
    if not get_uid_from_list(uid) == -1:
        access_uid_link_lock.acquire()
        access_uid_link.remove(uid)
        access_uid_link_lock.release()
    else:
        main_logger.info("Not del uid to list. uid[" + uid + "]")


def add_uid_to_dict(u_id,u_uid,state):
    # 不存在时追加到数据表
    if u_uid not in access_uid_dict:
        access_uid_dict_lock.acquire()
        access_uid_dict[u_uid] = \
            { 'id_key'      : u_id ,
              'process'     : None,
              'state'       : state,
              'is_running'  : 0
            }
        access_uid_dict_lock.release()
    #else:
    #    set_state_to_dict(u_id,u_uid, state)

def set_state_to_dict(u_id,u_uid,state):
    # 不存在时追加到数据表
    if u_uid in access_uid_dict:
        access_uid_dict_lock.acquire()
        access_uid_dict[u_uid]['state'] = state
        access_uid_dict_lock.release()
    #else:
    #    add_uid_to_dict(u_id, u_uid, state)

def get_uid_info_from_dict(u_uid):
    # 不存在时追加到数据表
    if u_uid in access_uid_dict:
        return access_uid_dict[u_uid]
    else:
        return None

def del_uid_from_dict(u_uid):
    # 不存在时追加到数据表
    if u_uid in access_uid_dict:
        access_uid_dict_lock.acquire()
        access_uid_dict.pop(u_uid)
        access_uid_dict_lock.release()

def set_running_state_to_dict(u_uid,state):
    # 不存在时追加到数据表
    if u_uid in access_uid_dict:
        access_uid_dict_lock.acquire()
        access_uid_dict[u_uid]['is_running'] = state
        access_uid_dict_lock.release()

############################################################
from enum import Enum
class uid_status(Enum):
    ready       = 0     # 新数据，未处理
    alloted     = 1     # 分配到计算服务器完成
    learning    = 10    # 学习中
    incr_learn  = 11    # 增量学习
    relearn     = 12    # 重新学习
    finished    = 20    # 学习完成
    checked     = 30    # 可以检测
    concerned   = 50    # 关注用户（业务待定）
    invalid     = 98    # 无效用户被声明(用于学习过程中，被停止学习用户发生
    invalid_end = 99    # 无效用户


############################################################
from DBUtils.PooledDB import PooledDB
import pymysql
import traceback

mydbspool = None

def init_mysql():
    db_config = {
        "host": main_cfg.get_value("mysql_host"),
        "port": int(main_cfg.get_value("mysql_port")),
        "user": main_cfg.get_value("mysql_user"),
        "passwd": main_cfg.get_value("mysql_passwd"),
        "db": main_cfg.get_value("mysql_db_video")
    }
    mydbspool = PooledDB(pymysql, 5, **db_config)
    return mydbspool

def get_uid_from_db(flg):
    nRet = None
    conn = None
    #连接池中获取连接
    try:
        conn = mydbspool.connection()
        if not conn:
            main_logger.error("Manager database cannot connected.")
            return nRet
    except Exception:
        main_logger.error("Mysql connect error (" + main_cfg.get_value("mysql_host") + ").")
        main_logger.error(traceback.format_exc())
        return nRet

    try:
        cur = conn.cursor()
        reCout = cur.execute(
            "SELECT id,uid FROM lstm_uid where client_host = '" + local_host_name + "' and state = " + str(flg.value) + ";"
        )
        if reCout > 0:
            nRet = cur.fetchall()
    except Exception:
        main_logger.error("MySQL data read error.")
        main_logger.error(traceback.format_exc())

    cur.close()
    conn.close()
    return nRet


def set_status_to_db(row_id,flg):
    nRet = False
    conn = None
    #连接池中获取连接
    try:
        conn = mydbspool.connection()
        if not conn:
            main_logger.error("Manager database cannot connected.")
            return False
    except Exception:
        main_logger.error("Mysql connect error (" + main_cfg.get_value("mysql_host") + ").")
        main_logger.error(traceback.format_exc())
        return False

    try:
        cur = conn.cursor()
        reCout = cur.execute(
            "UPDATE lstm_uid SET state = " + str(flg.value) + " where id = " + str(row_id) + ";")
        if reCout > 0:
            nRet = True
    except Exception:
        main_logger.error("MySQL data read error.")
        main_logger.error(traceback.format_exc())

    cur.close()
    conn.commit()
    conn.close()
    return nRet
