# /usr/bin/python
# -*- coding: utf-8 -*-

import os

from DBUtils.PooledDB import PooledDB
import pymysql

import signal
import time
from multiprocessing import Process, Queue

import json
import paramiko

import traceback

from elasticsearch import Elasticsearch

from util import const
from conf_process.config_josn import main_service_json_conf
from util.const_log import init_const
from util.const_log import init_log

main_access_run_flg = True


# def my_signal_handler(self,signum, frame):
def my_signal_handler(signum, frame):
    global main_access_run_flg
    main_access_run_flg = False
    print("main_access_run_flg :" + str(main_access_run_flg))
    # time.sleep(10)
    # exit(0)


class main_process(Process):
    ### 鍏ㄥ眬鍙橀噺瀹氫箟 ##############
    main_cfg = None
    logger = None
    mydbspool = None

    # 褰撳墠鍏ㄩ儴UID
    local_uid_info = {}
    not_push_uid = {}

    # 璁板綍鏈鐞哢ID
    new_uid_id = set()

    ###鐘舵€佺粺璁?
    status_start_time=time.time()

    status_client_cnt = 0
    status_uid_cnt = 0
    status_main_times = 0

    def __init__(self, arg):
        super(main_process, self).__init__()
        self.info_queue = arg

        ## init ###################
        init_const()

    def init_mysql(self):
        db_config = {
            "host": self.main_cfg.get_value("mysql_host"),
            "port": int(self.main_cfg.get_value("mysql_port")),
            "user": self.main_cfg.get_value("mysql_user"),
            "passwd": self.main_cfg.get_value("mysql_passwd"),
            "db": self.main_cfg.get_value("mysql_db_video"),

        }


        self.mydbspool = PooledDB(pymysql, 5, **db_config)

    def set_data_access_fag(self, from_flg, to_flg):
        # self.logger.info("set_data_access_fag:[" + str(from_flg) + "->" + str(to_flg) +"]");
        # func_start = time.time();
        conn = None;
        # 杩炴帴姹犱腑鑾峰彇杩炴帴
        try:
            conn = self.mydbspool.connection()

            if not conn:
                self.logger.error("Manager database cannot connected.")
                return False
        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return False

        try:
            cur = conn.cursor()
            reCout = cur.execute(
                "UPDATE users_userinformation SET sign = " + str(to_flg) + "  where sign = " + str(from_flg) + ";")
            cur.close()
            conn.commit();
            conn.close()

            if reCout == 0:
                # self.logger.info("No new_uid data in DB.")
                return False

        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return False
        # func_end = time.time()
        # self.logger.info("Update uid flag finish. ( elapsed: %f s ) " ,func_end - func_start)
        return True

    def get_uid_from_db(self):
        # self.logger.info("get_uid_from_db")
        # func_start = time.time()

        conn = None
        # 杩炴帴姹犱腑鑾峰彇杩炴帴
        try:
            conn = self.mydbspool.connection()

            if not conn:
                self.logger.error("Manager database cannot connected.")
                return False
        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return False

        try:
            cur = conn.cursor()
            reCout = cur.execute(
                "SELECT uid FROM users_userinformation where sign = 1;"
            )
            if reCout > 0:
                nRet = cur.fetchall()
                for item in nRet:
                    self.new_uid_id.add(str(item[0]))

            else:
                self.logger.info("No new_uid data in DB.")
                cur.close()
                conn.commit();
                conn.close()
                return False

            cur.close()
            conn.commit();
            conn.close()
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return False

        # func_end = time.time()
        # self.logger.info("New uid get finish. ( elapsed: %f s ) " ,func_end - func_start)

        return True

    # def load_local_uid(self):
    #     self.local_uid_info = {};
    #     for client_conf in self.main_cfg.get_value("clients"):
    #         self.local_uid_info[client_conf["client_host_name"]] = [];
    #
    #     uids_file_name = self.main_cfg.get_value("local_uid_path") + "/" + self.main_cfg.get_value("local_uid_filename")
    #     cont_txt = ''
    #
    #     if not os.path.exists(uids_file_name):
    #         self.logger.info("Local uid file is not exists.")
    #         return
    #
    #     try:
    #         ff = open(uids_file_name)
    #         if ff:
    #             cont_txt = ff.read()
    #             ff.close()
    #     except Exception:
    #         self.logger.error("Local uid file read error.")
    #         self.logger.error(traceback.format_exc())
    #         return
    #
    #     try:
    #         self.local_uid_info = json.loads(cont_txt)
    #     except Exception:
    #         self.logger.error("Json format error.")
    #         return


    # def save_local_uid(self):
    #     if self.local_uid_info == None or len(self.local_uid_info) == 0:
    #         return
    #     cont_txt = json.dumps(self.local_uid_info, indent=2)
    #     uids_file_name = self.main_cfg.get_value("local_uid_path") + "/" + self.main_cfg.get_value("local_uid_filename")
    #
    #     try:
    #         ff = open(uids_file_name, 'w')
    #         if ff:
    #             ff.write(cont_txt)
    #             ff.close()
    #     except Exception:
    #         self.logger.error("Local uid file write error.")
    #         self.logger.error(traceback.format_exc())

    # def load_no_access_uid(self):
    #     self.not_push_uid = {};
    #     for client_conf in self.main_cfg.get_value("clients"):
    #         client_host_name = client_conf["client_host_name"]
    #         self.not_push_uid[client_host_name] = [];
    #         uids_file_name = self.main_cfg.get_value("local_uid_path") + "/" + client_host_name
    #         cont_txt = ''
    #         if not os.path.exists(uids_file_name):
    #             self.logger.info("Local uid file is not exists.[" + client_host_name + "]")
    #             continue
    #         try:
    #             ff = open(uids_file_name, 'r')
    #             if ff:
    #                 cont_txt = ff.read()
    #                 ff.close()
    #         except Exception:
    #             self.logger.error("Local uid file read error.")
    #             self.logger.error(traceback.format_exc())
    #             continue
    #
    #         self.not_push_uid[client_host_name] = json.loads(cont_txt)

    # def save_no_access_uid(self, uids, client_name):
    #     try:
    #         ff = open(client_name, 'w')
    #         if ff:
    #             ff.write(uids)
    #             ff.close()
    #     except Exception:
    #         self.logger.error("Local uid file write error.")
    #         self.logger.error(traceback.format_exc())

    # def get_min_cilent(self):
    #     if self.local_uid_info == None or len(self.local_uid_info) == 0:
    #         return None
    #
    #     client_name = ''
    #     for cur_client in self.local_uid_info:
    #         if len(client_name) == 0 \
    #                 or len(self.local_uid_info[client_name]) > len(self.local_uid_info[cur_client]):
    #             client_name = cur_client;
    #
    #     return client_name

    def uid_is_exist(self, uid, chk_obj):
        for client in chk_obj:
            for item in client:
                if uid in item:
                    return True
        return False

    def get_client_filepath(self, client_name):
        filepath = ''
        for client_conf in self.main_cfg.get_value("clients"):
            if client_conf["client_host_name"] == client_name:
                filepath = client_conf["client_uid_path"]
                break;
        return filepath


    # def get_client_ip(self, client_name):
    #     client_ip = '127.0.0.1'
    #     for client_conf in self.main_cfg.get_value("clients"):
    #         if client_conf["client_host_name"] == client_name:
    #             client_ip = client_conf["client_ip"]
    #             break;
    #
    #     return client_ip

    def deploy_uid_to_client(self, src_fn, dsc_hostip, dsc_fn):
        try:
            private_key = paramiko.RSAKey.from_private_key_file('/root/.ssh/id_rsa')
            slink = paramiko.Transport((dsc_hostip, 22))
            slink.connect(username='root', pkey=private_key)
            sftp = paramiko.SFTPClient.from_transport(slink)
            sftp.put(src_fn, dsc_fn)
            slink.close()
        except:
            self.logger.error(
                "deploy_uid_to_client error.[ sftp: " + dsc_hostip + "put " + src_fn + " to: " + dsc_fn + "] ")
            return False

        self.logger.info(
            "deploy_uid_to_client ok.[ sftp: " + dsc_hostip + "put " + src_fn + " to: " + dsc_fn + "] ")
        return True

    def make_status_data(self):
        status_data = json.loads('{}')
        status_data["PID"] = str(self.pid)
        status_data["Pname"] = self.name

        status_data["run_time"] = (time.time() - self.status_start_time)

        status_data["status_client_cnt"] = self.status_client_cnt
        status_data["status_uid_cnt"] = self.status_uid_cnt
        status_data["status_main_times"] = self.status_main_times

        return json.dumps(status_data, indent=2)

    def chk_create_lstm_uid_table(self):

        conn = None;
        # 杩炴帴姹犱腑鑾峰彇杩炴帴
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return False
        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return False

        try:
            sql_txt = 'CREATE TABLE IF NOT EXISTS lstm_uid( \
            	        id int auto_increment primary key, \
                        uid varchar(240) not null unique , \
                        client_host varchar(64), \
                        state int default 0, \
                        learn_start_date datetime, \
                        learn_start_posi varchar(240), \
                        learned_cnt int default 0);'
            cur = conn.cursor()
            reCout = cur.execute(sql_txt)
            cur.close()
            conn.commit();
            conn.close()
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return False

        return True

    def move_uid_to_lstm_table(self):

        conn = None;
        # 杩炴帴姹犱腑鑾峰彇杩炴帴
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return False
        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return False

        try:
            sql_txt = "INSERT INTO lstm_uid(uid) SELECT tid FROM users_userinformation WHERE sign = 1;"
            cur = conn.cursor()
            reCout = cur.execute(sql_txt)
            cur.close()
            conn.commit();
            conn.close()
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return False

        return True

    def get_no_access_data(self):
        no_access_data = None
        conn = None;
        # 杩炴帴姹犱腑鑾峰彇杩炴帴
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return None
        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return None

        try:
            sql_txt = "SELECT id,uid,client_host FROM lstm_uid where state = 0;"
            cur = conn.cursor()
            if cur.execute(sql_txt) > 0:
                no_access_data = cur.fetchall()
            cur.close()
            conn.commit();
            conn.close()
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return None

        return no_access_data

    def get_client_distribution(self):
        client_dist = None
        conn = None;
        # 杩炴帴姹犱腑鑾峰彇杩炴帴
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return None
        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return None

        try:
            sql_txt = "SELECT client_host,count(client_host) as cnt \
                       FROM lstm_uid \
                       WHERE state <> 0 and state <> 99 \
                       GROUP BY client_host \
                       ORDER BY cnt ASC;"
            cur = conn.cursor()
            if cur.execute(sql_txt) > 0:
                client_dist = cur.fetchall()
            cur.close()
            conn.commit();
            conn.close()
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return None

        return client_dist

    def get_suit_client(self, used_cl):
        # 宸蹭娇鐢ㄥ鎴风鏈夊簭鍒楄〃杞崲涓簊et闆嗗悎
        used_cl_set = set(used_cl)

        # 閰嶇疆鏂囦欢涓寚瀹氬彲鐢ㄥ叏閮ㄥ鎴风闆嗗悎
        all_cl_set = set()
        for client_conf in self.main_cfg.get_value("clients"):
            all_cl_set.add(client_conf["client_host_name"])

            # 鏈敤闆?宸泦) = 鍏ㄩ儴鍙敤闆?- 浠ョ敤闆?
            unused_cl = all_cl_set.difference(used_cl_set)
          
            # 瀛樺湪鏈敤瀹㈡埛绔?
            if len(unused_cl) > 0:
                return unused_cl.pop()

        # 鍏ㄩ儴琚娇鐢ㄦ椂锛岄€夊彇UID鏈€灏戝鎴风
        all_cl_list = list(all_cl_set)
        all_cl_list.sort(key=used_cl.index)
        return all_cl_list[0]

    def set_client_and_state(self, client_name, row_id):
        conn = None;
        # 杩炴帴姹犱腑鑾峰彇杩炴帴
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return False
        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return False

        try:
            cur = conn.cursor()
            reCout = cur.execute(
                "UPDATE lstm_uid SET client_host = '" + client_name + "' , state = 1 where id = " + str(row_id) + ";")
            cur.close()
            conn.commit();
            conn.close()

            if reCout == 0:
                # self.logger.info("No new_uid data in DB.")
                return False

        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return False

        return True
    def change_uid_incr_learn_state(self, cur_uid):
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")

        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
        try:
            sql_txt = "UPDATE lstm_uid SET state=11 WHERE state=30 and uid='"+str(cur_uid)+"';"

            cur = conn.cursor()
            reCout = cur.execute(sql_txt)
            conn.commit()
            if not reCout is None:
                self.logger.info("uid state changed,incr_learn started")
            cur.close()


        except Exception:
            self.logger.error("MySQL data read error or no such uid.")
            self.logger.error(traceback.format_exc())
        conn.close()
    def set_incr_learn_finished(self,cur_uid):
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")

        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
        try:
            sql_txt = "UPDATE lstm_prediction SET incr_learn=3 WHERE incr_learn=1 and uid='"+cur_uid+"';"

            cur2 = conn.cursor()
            reCout2 = cur2.execute(sql_txt)
            conn.commit()
            if reCout2>0:
                self.logger.info("incr_learn finished")
            cur2.close()

            conn.close()
        except Exception:
            self.logger.error("MySQL data read error or no such uid.")
            self.logger.error(traceback.format_exc())
            cur2.close()
            conn.close()
    # 修改
    def get_incr_learn_data(self,cur_uid,happened_start_posi,happened_end_posi):
        connection=None
        self.logger.info("start es access data")
        try:
            es_host = self.main_cfg.get_value("es_url")
            es_port = self.main_cfg.get_value("es_port")
            es = Elasticsearch([{
                "host": es_host,
                "port": es_port
            }])
            body = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"status": "2"}},
                            {"match": {"tid": cur_uid}}
                        ]
                    }
                }
            }
            result = es.search(index="lstm", body=body)
            limit=result["hits"]["total"]
            body = {
                "sort": [{
                    "occ_time": {"order": "asc"}
                }],
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"status": "2"}},
                            {"match": {"tid": cur_uid}}
                        ]
                    }
                },
                "size":limit
            }
            result = es.search(index="lstm", body=body)
        except:
            self.logger.error("es connect error.")
            self.logger.error(traceback.format_exc())

        #row_prefix_str = bytes(cur_uid, 'utf-8')
        for list in result["hits"]["hits"]:
            key = list["_id"]
            es.update(index="lstm", doc_type="_doc", id=key, body={"doc": {"status": "3"}})
        self.logger.info("access finished")

    def get_start_end_posi(self,cur_uid):
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")

        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
        try:
            sql_txt2 = "select happened_start_posi,happened_end_posi from lstm_prediction where incr_learn=1 and uid='"+str(cur_uid)+"';"

            cur = conn.cursor()
            if not cur.execute(sql_txt2) is None:
                happened_posi = cur.fetchall()
            cur.close()
            conn.commit()
            conn.close()
            self.logger.info("get happened position finished")
        except Exception:
            self.logger.error("MySQL data read error or no such uid.")
            self.logger.error(traceback.format_exc())
            conn.close()
            return None
        return happened_posi

    def start_uid_incr_learn(self):
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")

        except Exception:
            self.logger.error("Mysql connect error (" + self.main_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
        #self.logger.info("checking uid incr_learn state")
        try:
            sql_txt='select uid,count(incr_learn) from lstm_prediction where incr_learn=1 group by uid'

            cur=conn.cursor()
            cur2=conn.cursor()
            cur3=conn.cursor()
            result=cur.execute(sql_txt)
            cur_uid_list=[]
            if not result is None:
                res_tuple=cur.fetchall()
                res_li=list(res_tuple)
                if len(res_li):
                    for re_list in res_li:
                        if re_list[1] >= 1:
                            posi_iter = self.get_start_end_posi(re_list[0])
                            self.change_uid_incr_learn_state(re_list[0])
                            time.sleep(1)
                            for posi in posi_iter:
                                self.get_incr_learn_data(re_list[0], posi[0], posi[1])
                                self.set_incr_learn_finished(re_list[0])
                            self.logger.info("incr_learn prepared successfully")

            sql_txt2 = "select a.a_uid from (select uid as a_uid,count(incr_learn) as a_count from lstm_prediction where incr_learn=3 group by uid) as a where a.a_count>=1"
            result2 = cur2.execute(sql_txt2)
            if not result2 is None:
                cur_uid_li = cur2.fetchall()
                for cur_uid in cur_uid_li:
                    cur_uid=list(cur_uid)[0]
                    sql_txt3 = "select state from lstm_uid where uid='" + cur_uid + "';"
                    result3 = cur3.execute(sql_txt3)
                    state_result=cur3.fetchall()
                    state_result=list(state_result[0])
                    if state_result[0]==30 :
                        self.change_uid_incr_learn_state(cur_uid)

        except pymysql.err.ProgrammingError as e:
            if e.args[0]!=1146:
                self.logger.error("read data error")
                self.logger.error(traceback.format_exc())
        except:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())

        cur.close()
        cur2.close()
        cur3.close()
        conn.close()



    def run(self):
        self.logger = init_log(const.PROJECT_NAME + '.' + str(self.name))
        self.logger.info("=================================")
        self.logger.info("Start access log")

        self.main_cfg = main_service_json_conf(const.JSON_CFG_FILE_NAME)

        ##set signal handler
        signal.signal(signal.SIGTERM, my_signal_handler)
        signal.signal(signal.SIGINT, my_signal_handler)

        self.init_mysql()
        self.chk_create_lstm_uid_table()

        report_time = time.time()

        global main_access_run_flg

        while main_access_run_flg:
            self.logger.info("start check incr_learn")

            self.start_uid_incr_learn()

            # 鏀堕泦鏈繘绋嬬姸鎬佷俊鎭紝鍙戦€佸埌绠＄悊杩涚▼
            if self.info_queue:
                # if True:
                if (time.time() - report_time) > 300:
                    self.info_queue.put(self.make_status_data())
                    report_time = time.time()
            # 娓呯┖鍏ㄥ眬鍙橀噺
            self.db_data_id = ''
            self.starttm = ''
            self.duration = ''

            # 浠庣敤鎴蜂俊鎭〃涓幏鍙栨柊鐢ㄦ埛锛屽苟鎻掑叆鍒癓STM绠楁硶鏁版嵁绠＄悊琛ㄤ腑

            self.set_data_access_fag(0, 1)

            self.move_uid_to_lstm_table();

            self.set_data_access_fag(1, 2)

            # 杩炴帴鍒扮鐞嗘暟鎹簱锛岃幏鍙栨柊澧濽ID鏁版嵁
            # 鑾峰彇澶辫触鎴栨棤鍙鐞嗘暟鎹椂锛岀潯鐪?0绉?

            no_access_data = self.get_no_access_data()
            if no_access_data == None:

                time.sleep(10)
                continue

            # 閫愭潯澶勭悊鏁版嵁
            for usr_data in no_access_data:
                d_id = usr_data[0]

                client_dist = self.get_client_distribution()
                
                used_cl = []
                if not client_dist == None:
                    for cl in client_dist:
                        if cl[0] == None:
                            break;
                        used_cl.append(cl[0])
                cl_name = self.get_suit_client(used_cl)
                
                self.set_client_and_state(cl_name, d_id)

        self.logger.info("Main process stoped.")

#vap = main_process(None)
#vap.run()
