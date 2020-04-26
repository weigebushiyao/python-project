#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
from DBUtils.PooledDB import PooledDB
import pymysql
import datetime
import json
import signal
import time
from   multiprocessing import Process,Queue
import csv
import numpy as np
from elasticsearch import Elasticsearch
from   util import const
from   conf_process.config_josn import client_service_json_conf
from   util.const_log import init_const
from   util.const_log import init_log
import pytz
from   my_process.macdlp_lstm import macdlp_lstm
import traceback
import tensorflow as tf
client_access_run_flg = True
# def my_signal_handler(self,signum, frame):
def my_signal_handler(signum, frame):
    global client_access_run_flg
    client_access_run_flg = False
    print("client_access_run_flg :"+str(client_access_run_flg ))
    #time.sleep(10)
    #exit(0)

class client_process(Process):
    ### 全局变量定义 ##############
    client_cfg = None
    logger = None
    mydbspool = None
    global_app_id = dict()
    global_information_id=dict()
    ###########################################
    # 进程管理使用
    # 运行状态（重要）
    # 进程状态run_status：
    # 0: 空闲      主进程初始化或进程检测时设置
    # 1: 等待开启  启动uid学习时，进程管理线程设置
    # 2: 运行中    算法子进程开始学习工作时设置
    # 3: 等待结束  算法子进程学习完成时设置，此时可被回收，再次使用
    # -1:异常      子进程异常，不能工作
    #run_status = 0
    # 处理UID （重要）
    # active_uid =''
    ###########################################

    #记录未处理UID
    new_uid_id = set()
    ###状态统计
    status_start_time=time.time()
    status_client_cnt=0
    status_uid_cnt = 0
    status_main_times = 0

    def __init__(self,arg_info_queue,arg_run_status,arg_active_uid):
        super(client_process, self).__init__()
        self.info_queue = arg_info_queue
        self.run_status = arg_run_status
        self.active_uid = arg_active_uid

        ## init ###################
        init_const()
    def init_mysql(self):
        db_config = {
            "host": self.client_cfg.get_value("mysql_host"),
            "port": int(self.client_cfg.get_value("mysql_port")),
            "user": self.client_cfg.get_value("mysql_user"),
            "passwd": self.client_cfg.get_value("mysql_passwd"),
            "db": self.client_cfg.get_value("mysql_db_video")
        }
        self.mydbspool = PooledDB(pymysql, 5, **db_config)
    def make_status_data(self):
        status_data = json.loads('{}')
        status_data["PID"] = str(self.pid)
        status_data["Pname"] = self.name

        status_data["run_time"] = (time.time() - self.status_start_time)

        status_data["status_client_cnt"] = self.status_client_cnt
        status_data["status_uid_cnt"] = self.status_uid_cnt
        status_data["status_main_times"] = self.status_main_times

        return json.dumps(status_data, indent=2)
    ################################################################################################################
    ## 获取学习数据
    ################################################################################################################
    # 初始化全局分配AppID用存储过程
    def init_new_app_id_pro(self):
        create_procedure = '''
        CREATE PROCEDURE `new_app_id`(IN app_name_param varchar(128))
        BEGIN
        
                DECLARE p_sqlcode INT DEFAULT 0;
                DECLARE ret_app_id INT;
                
                DECLARE duplicate_key CONDITION FOR 1062;
                DECLARE CONTINUE HANDLER FOR duplicate_key
                BEGIN
                        SET p_sqlcode=1052;
                END;
        
                select @MAX_APPID:=(ifnull(max(app_id),0) + 1) from global_lstm_app_id
                        where is_stop = 0;
        
                insert into global_lstm_app_id ( app_name,app_id)
                        values( app_name_param, @MAX_APPID);
        
                set ret_app_id = @MAX_APPID;
        
                IF p_sqlcode<>0 THEN
                        SELECT @APP_ID_TMP:=app_id FROM global_lstm_app_id
                                WHERE app_name = app_name_param AND is_stop = 0;
                        set ret_app_id = @APP_ID_TMP;
                END IF;
                
                select ret_app_id;
        END;
        '''

        conn = None
        #连接池中获取连接
        try:
            conn = self.mydbspool.connection()

            if not conn:
                self.logger.error("Manager database cannot connected.")
                return False
        except:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return False

        try:
            cur = conn.cursor()
            reCout = cur.execute(create_procedure)
            cur.close()
            conn.commit()
        except pymysql.InternalError as e:
            cur.close()
            conn.close()
            # 已经存在，不产生错误
            if e.args[0] != 1304:
                self.logger.error("Mysql create procedure (new_app_id) error.")
                return False
        return True
    #初始化全局分配information_id用存储过程
    def init_new_information_id_pro(self):
        create_procedure = '''
                CREATE PROCEDURE `new_information_id`(IN information_lable_param varchar(128))
                BEGIN

                        DECLARE p_sqlcode INT DEFAULT 0;
                        DECLARE ret_information_id INT;

                        DECLARE duplicate_key CONDITION FOR 1062;
                        DECLARE CONTINUE HANDLER FOR duplicate_key
                        BEGIN
                                SET p_sqlcode=1052;
                        END;

                        select @MAX_INFORMATIONID:=(ifnull(max(information_id),0) + 1) from global_information_id
                                ;

                        insert into global_information_id (information_lable,information_id)
                                values(information_lable_param,@MAX_INFORMATIONID);

                        set ret_information_id = @MAX_INFORMATIONID;

                        IF p_sqlcode<>0 THEN
                                SELECT @INFORMATION_ID_TMP:=information_id FROM global_information_id
                                        WHERE information_lable = information_lable_param ;
                                set ret_information_id = @INFORMATION_ID_TMP;
                        END IF;

                        select ret_information_id;
                END;
                '''
        conn = None
        # 连接池中获取连接
        try:
            conn = self.mydbspool.connection()

            if not conn:
                self.logger.error("Manager database cannot connected.")
                return False
        except:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return False

        try:
            cur = conn.cursor()
            reCout = cur.execute(create_procedure)
            cur.close()
            conn.commit()
        except pymysql.InternalError as e:
            cur.close()
            conn.close()
            # 已经存在，不产生错误
            if e.args[0] != 1304:
                self.logger.error("Mysql create procedure (new_information_id) error.")
                return False
        return True
    # 读取数据库中已分配AppID
    def init_app_id_from_db(self):
        conn = None
        #连接池中获取连接
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return False
        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return False

        try:
            sql_txt = 'CREATE TABLE IF NOT EXISTS global_lstm_app_id( \
                        app_name varchar(128) primary key, \
                        app_id int unique  , \
                        is_stop int default 0, \
                        memo varchar(256));'
            cur = conn.cursor()
            reCout = cur.execute(sql_txt)
            cur.close()
            conn.commit()
        except:
            self.logger.error("Mysql create table (global_lstm_app_id) error.")
            cur.close()
            conn.close()
            return False

        try:
            cur = conn.cursor()
            reCout = cur.execute(
                "SELECT app_name,app_id FROM global_lstm_app_id WHERE is_stop = 0;"
            )
            if reCout > 0:
                nRet = cur.fetchall()
                for item in nRet:
                    self.global_app_id[str(item[0])] = item[1]
            cur.close()
            conn.close()
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return False

        return True
    #获取数据库中information_id
    def init_information_id_from_db(self):
        try:
            conn=self.mydbspool.connection()
            if not conn:
                self.logger.error("No mysql connection.")
                return False
        except Exception:
            self.logger.error("Mysql connection error.")
            self.logger.error(traceback.format_exc())
            return False
        try:
            sql_txt = "create table if not exists global_information_id(information_lable varchar(240) not null unique,information_id int auto_increment unique ); "
            cur=conn.cursor()
            re=cur.execute(sql_txt)
            cur.close()
            conn.commit()
        except:
            self.logger.error("Create table global_information_id failed.")
            cur.close()
            conn.close()
            return False
        try:
            cur=conn.cursor()
            sql_req=cur.execute("select information_lable,information_id from global_information_id")
            if sql_req>0:
                re_iter=cur.fetchall()
                for ele in re_iter:
                    self.global_information_id[str(ele[0])]=ele[1]
            cur.close()
            conn.close()
        except Exception:
            self.logger.error("Mysql data query fails")
            cur.close()
            conn.close()
            return False
        return True
    def make_global_information_id(self,information_lable):
        information_id=-1
        conn=None
        try:
            conn=self.mydbspool.connection()
            if not conn:
                self.logger.error("No mysql connection.")
                return information_id
        except Exception:
            self.logger.error("Mysql connection error.")
            self.logger.error(traceback.format_exc())
            return information_id
        try:
            cur=conn.cursor()

            sql_ret=cur.execute("insert into global_information_id(information_lable) values('"+information_lable+"');")
            query_re = cur.execute("select information_id from global_information_id where information_lable='"+information_lable+"';")
            conn.commit()
            if query_re:
                re=cur.fetchall()
                information_id=re[0][0]
            cur.close()
            conn.close()
        except Exception:
            self.logger.error("Mysql read data error.")
            self.logger.error(traceback.format_exc())
            return information_id
        return information_id
    # 分配AppID，全局使用
    def make_global_app_id(self,app_name):
        app_id = -1
        conn = None
        #连接池中获取连接
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return app_id
        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return app_id
        try:
            cur = conn.cursor()
            ret = cur.execute("call new_app_id('" + app_name +"');")
            if ret :
                nRet = cur.fetchall()
                app_id = int(nRet[0][0])
            cur.close()
            conn.commit()
            conn.close()
        except Exception:
            # 异常原因可能由其他
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return app_id
        return app_id
    # 获取AppID，不存在时分配
    def get_app_id(self,app_name):
        app_id= -1
        try:
            if app_name in self.global_app_id:
                app_id = self.global_app_id[app_name]
            else:
                # app id 未分配时，从数据库统一分配
                app_id = self.make_global_app_id(app_name)
                # 被其他服务器注册的可能性大，再尝试一次
                if app_id == -1:
                    app_id = self.make_global_app_id(app_name)
                    # 失败处理：1.数据丢弃 2.崩溃处理  暂时采用方法1
                    if app_id  == -1:
                        pass
                self.global_app_id[app_name]=app_id
        except:
            self.logger.error("make app id error.")

        return app_id
    def get_information_id(self,information_lable):
        information_id=-1
        try:
            if information_lable in self.global_information_id:
                information_id=self.global_information_id[information_lable]
            else:
                information_id=self.make_global_information_id(information_lable)
                if information_id==-1:
                    information_id=self.make_global_information_id(information_lable)
                    if information_id==-1:
                        pass
                self.global_information_id[information_lable]=information_id
        except:
            self.logger.error("make information id error.")
        return information_id

    def get_occtime_level_id(self, timestamp):
        time=timestamp.replace("T"," ")
        time=time[0:19]
        hour_time =  datetime.datetime.strptime(time,"%Y-%m-%d %H:%M:%S").strftime("%H")
        hour_time = int(hour_time)
        if 22 <= hour_time or 0 <= hour_time < 8:
            occtime_level_id = 201
        elif 8 <= hour_time < 10:
            occtime_level_id = 202
        elif 10 <= hour_time < 12:
            occtime_level_id = 203
        elif 12 <= hour_time < 14:
            occtime_level_id = 204
        elif 14 <= hour_time < 16:
            occtime_level_id = 205
        elif 16 <= hour_time < 18:
            occtime_level_id = 206
        elif 18 <= hour_time < 20:
            occtime_level_id = 207
        elif 20 <= hour_time < 22:
            occtime_level_id = 208
        return occtime_level_id

    def get_ipcount_level_id(self, ipcount):
        ipcount = int(ipcount)
        if ipcount == 0:
            ipcount_level_id = 209
        elif 1 <= ipcount <= 3:
            ipcount_level_id = 210
        elif 4 <= ipcount <= 6:
            ipcount_level_id = 211
        elif 7 <= ipcount <= 10:
            ipcount_level_id = 212
        else:
            ipcount_level_id = 213
        return ipcount_level_id

    def get_exectime_level_id(self, exectime):
        exectime = int(exectime)
        if 0 <= exectime < 30:
            exectime_level_id = 214
        elif 30 <= exectime < 600:
            exectime_level_id = 215
        elif 600 <= exectime < 1800:
            exectime_level_id = 216
        elif 1800 <= exectime < 3600:
            exectime_level_id = 217
        else:
            exectime_level_id = 218
        return exectime_level_id

    def get_information_lable(self,app_id, occtime_level_id, ipcount_level_id, exectime_level_id):
        information_lable = str(app_id) +'-'+str(occtime_level_id) +'-'+ str(ipcount_level_id) +'-'+ str(exectime_level_id)
        return information_lable

    # def get_information_id(self,information_lable):
    #     try:
    #         conn=self.mydbspool.connection()
    #         cur=conn.cursor()
    #         sql_txt = "select information_id from global_information_id where information_lable='" + information_lable + "';"
    #         query_re=cur.execute(sql_txt)
    #         if query_re:
    #             iter_re=cur.fetchall()
    #             return iter_re[0][0]
    #         cur.close()
    #         conn.commit()
    #         conn.close()
    #     except Exception:
    #         self.logger.error("Mysql read data error.")
    #         self.logger.error(traceback.format_exc())
    #         cur.close()
    #         conn.close()

    # def get_information_matrix(self,appid, occtime_level_id, ipcount_level_id, exectime_level_id):
    #     information_matrix = np.zeros((4, 200), dtype=np.float32)
    #     information_matrix[0, appid] = float(1)
    #     information_matrix[1, occtime_level_id] = float(1)
    #     information_matrix[2, ipcount_level_id] = float(1)
    #     information_matrix[3, exectime_level_id] = float(1)
    #     return information_matrix

    def get_information_vector(self,information_id):
        if information_id>0:
            information_vector = np.zeros((1, 40000), dtype=np.float32)
            information_vector[0, information_id] = float(1)
            return list(information_vector[0])
        else:
            return None
    # 获取学习进度
    def get_learn_progress(self):
        learn_start_date = None
        learn_start_posi = None
        learned_cnt = 0

        conn = None
        # 连接池中获取连接
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return None,None,None
        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return None,None,None

        try:
            cur = conn.cursor()
            reCout = cur.execute(
                "SELECT learn_start_date,learn_start_posi,learned_cnt,state FROM lstm_uid \
                    WHERE uid = '" + str(self.active_uid.value,'utf-8') + "';" )
            if reCout > 0:
                nRet = cur.fetchall()
                learn_start_date = nRet[0][0]
                learn_start_posi = nRet[0][1]
                learned_cnt = nRet[0][2]
                learned_state=nRet[0][3]
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())

        cur.close()
        conn.close()
        return learn_start_date,learn_start_posi,learned_cnt,learned_state
    # 保存学习进度
    def set_learn_progress(self,learn_start_date,learn_start_posi,learn_cnt):
        nret = False
        conn = None
        # 连接池中获取连接
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return None,None,None
        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return None,None,None

        try:
            cur = conn.cursor()
            str_learn_start_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if not learn_start_date == None:
                str_learn_start_date = learn_start_date
                # if type(learn_start_date) == bytes:
                #     tz=pytz.timezone('Asia/Shanghai')
                #     dt_learn_start_date = datetime.datetime.fromtimestamp(
                #                                 int(str(learn_start_date,'utf-8')),tz).strftime("%Y-%m-%d %H:%M:%S")
                # str_learn_start_date = "'" + dt_learn_start_date + "'"
            str_learn_start_posi = 'Null'
            if not learn_start_posi == None:
                str_learn_start_posi = learn_start_posi

            uid=self.active_uid.value
            uid=uid.decode(encoding='utf-8')
            sql_txt = "UPDATE lstm_uid set learned_cnt = '" + str(learn_cnt) + "', \
                    learn_start_date ='" + str(str_learn_start_date) + "', \
                    learn_start_posi ='" + str(str_learn_start_posi) + "' \
                    WHERE uid ='" + uid + "';"
            reCout = cur.execute(sql_txt)
            if reCout > 0:
                nret = True
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())

        cur.close()
        conn.commit()
        conn.close()
        return nret
    # 保存学习完成状态
    def set_learn_finished(self):
        nret = False
        conn = None
        # 连接池中获取连接
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return None,None,None
        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return None,None,None

        try:
            cur = conn.cursor()
            reCout = cur.execute(
                "UPDATE lstm_uid set state = 30 \
                    WHERE uid = '" + str(self.active_uid.value,'utf-8') + "';" )
            if reCout > 0:
                nret = True
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())

        cur.close()
        conn.commit()
        conn.close()
        return nret
    # 获取增量学习条件
    def get_incr_learned_state(self):
        happend_time= None
        happend_posi = None
        data_len = 0

        conn = None
        # 连接池中获取连接
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return None,None,None
        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return None,None,None

        try:
            cur = conn.cursor()
            reCout = cur.execute(
                "SELECT state FROM lstm_uid \
                    WHERE uid = '" + str(self.active_uid.value,'utf-8') + "';" )
            if reCout > 0:
                nRet = cur.fetchall()
                learn_state = nRet[0][0]

        except pymysql.err.ProgrammingError as e:
            if e.args[0] != 1146:
                self.logger.error("MySQL data read error.")
                self.logger.error(traceback.format_exc())
        except:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())

        cur.close()
        conn.close()
        return learn_state
    def get_incr_learn(self):
        happend_time= None
        happend_posi = None
        data_len = 0

        conn = None
        # 连接池中获取连接
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")
                return None,None,None
        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
            return None,None,None

        try:
            cur = conn.cursor()
            reCout = cur.execute(
                "SELECT happened_time,happened_start_posi,data_len FROM lstm_prediction \
                    WHERE uid = '" + str(self.active_uid.value,'utf-8') + "' " \
                           + " and incr_learn = 1;" )
            if reCout > 0:
                nRet = cur.fetchall()
                happend_time = nRet[0][0]
                happend_posi = nRet[0][1]
                data_len = nRet[0][2]
        except pymysql.err.ProgrammingError as e:
            if e.args[0] != 1146:
                self.logger.error("MySQL data read error.")
                self.logger.error(traceback.format_exc())
        except:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())

        cur.close()
        conn.close()
        return happend_time,happend_posi,data_len

    # def matrix_conv_to_vector(self,information_matrix):
    #     convolution_kernel = tf.constant([1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1], shape=[4, 4, 1, 1],
    #                                      dtype=tf.float32)
    #     information_matrix = tf.convert_to_tensor(information_matrix)
    #     m = tf.reshape(information_matrix, shape=[1, 4, 200, 1])
    #     with tf.Session() as sess:
    #         sess.run(tf.global_variables_initializer())
    #         vec = tf.nn.conv2d(input=m, filter=convolution_kernel, strides=[1, 1, 1, 1], padding='VALID',
    #                                data_format='NHWC')
    #         numpy_array = vec.eval()
    #         reshaped_array = np.reshape(numpy_array, [1, 197])
    #         input_vector = list(reshaped_array[0])
    #     return input_vector
    def get_input_vector(self,app_id,occtime_level_id,ipcount_level_id,exectime_level_id):
        matrix = np.zeros((1, self.client_cfg.get_value("lstm_input_size")))
        matrix[:,app_id]=1
        matrix[:,occtime_level_id]=1
        matrix[:,ipcount_level_id]=1
        matrix[:,exectime_level_id]=1
        input_vector=list(matrix[0])
        return input_vector
    def get_data_from_es(self,learn_start_date,learn_start_posi,learn_cnt,all_get):
        self.logger.info("get data from es")
        # 初始化输出文件
        path_train = self.client_cfg.get_value("train_data") + '/' + str(self.active_uid.value, 'utf-8') + '/'
        if not os.path.exists(path_train):
            os.makedirs(path_train)

        csvfile_train = None
        try:
            path_train += "train_data"
            csvfile_train = open(path_train, 'a+', newline='')
            writer_vector = csv.writer(csvfile_train)

        except:
            if csvfile_train:
                csvfile_train.close()

            self.logger.error("save csv vector data error.")
            self.logger.error(traceback.format_exc())
            return 0, 0, None, None, None
        try:
            es_host = self.client_cfg.get_value("es_host")
            es_port = self.client_cfg.get_value("es_port")
            es = Elasticsearch([{
                "host": es_host,
                "port": es_port
            }])
            self.logger.info("sucess connect es")
        except:
            self.logger.error("es connect error.")
            self.logger.error(traceback.format_exc())
            return 0, None, None
            # 从配置文件中获取最大支持App数量

            # vector_len=200
            # 获取当前数据数量（数据文件行数）

        pre_csv_row_cnt = 0
        try:
            pre_csv_row_cnt = len(open(path_train, "r").readlines())
        except:
            pass

        try:
            b_uid = self.active_uid.value
            b_uid = b_uid.decode(encoding='utf-8')
            filter_str = None
            row_start_str = None
            row_prefix_str = None
            cfg_limit = None
            self.logger.info("get data uid" + "[" + str(b_uid) + "]")
            try:
                if self.client_cfg.get_value('data_extract_len'):
                    cfg_limit = int(self.client_cfg.get_value('data_extract_len'))
            except:
                self.logger.error("configure error[data_extract_len].")
            # 全量学习,增量学习 ( 数据范围检索条件：起始行 + 长度 ）
            if all_get == 1:
                # 特殊场景抽取2条数据
                if cfg_limit <= 1:
                    cfg_limit = 2
                body={
                    "query":{
                        "bool":{
                            "must":[
                                {"match":{"status": "0"}},
                                {"match":{"tid":b_uid}}
                            ]
                        }
                    },
                    "size": cfg_limit
                }

            else:

                body={
                    "sort": [{
                        "occ_time": {"order": "asc"}
                    }],
                    "query":{
                        "bool":{
                            "must":[
                                {"match":{"status": "0"}},
                                {"match":{"tid":b_uid}}
                            ]
                        }
                    },
                    "size": cfg_limit
                }

            total_cur_data_cnt = 0
            key_list = []
            self.logger.info("start get data from es")
            while True:

                result=es.search(index="lstm",body=body)

                cur_data_cnt = 0
                pre_app_id = None
                occtime= None
                exectime=None
                ip_count=None
                app_name=None
                input_vector=None
                information_vector=None
                inout_data=None

                for list in result["hits"]["hits"]:
                    cur_data_cnt = cur_data_cnt+1
                    key=list["_id"]
                    key_list.append(key)
                    lis=list["_source"]
                    time=lis["occ_time"]
                    time=time.replace('T',' ')[0:19]
                    if learn_start_posi == None or learn_start_posi == '':
                        learn_start_posi = b_uid
                        learn_start_date = time
                    if pre_app_id == None:
                        exec_time_level_id = self.get_exectime_level_id(lis["apprunning_time"])
                        cur_occtime_levle_id = self.get_occtime_level_id(time)
                        ip_count_level_id = self.get_ipcount_level_id(lis["ip_count"])
                        app_id = self.get_app_id(lis["app_name"])
                        information_lable = self.get_information_lable(app_id, cur_occtime_levle_id,
                                                               ip_count_level_id, exec_time_level_id)
                        information_id = self.get_information_id(information_lable)
                        input_vector = self.get_input_vector(app_id, cur_occtime_levle_id, ip_count_level_id,
                                                     exec_time_level_id)
                        pre_app_id = app_id
                        occtime = time
                        exectime = lis["apprunning_time"]
                        ip_count = lis["ip_count"]
                        app_name = lis["app_name"]
                    else:
                        exec_time_level_id = self.get_exectime_level_id(lis["apprunning_time"])
                        cur_occtime_levle_id = self.get_occtime_level_id(time)
                        ip_count_level_id = self.get_ipcount_level_id(lis["ip_count"])
                        app_id = self.get_app_id(lis["app_name"])
                        information_lable = self.get_information_lable(app_id, cur_occtime_levle_id, ip_count_level_id,
                                                               exec_time_level_id)
                        information_id = self.get_information_id(information_lable)
                        information_vector = self.get_information_vector(information_id)
                        inout_data = input_vector + information_vector + [app_name] + [occtime] + [ip_count] + [exectime]
                        # csvfile_train = open(path_train, 'a+', newline='')
                        # writer_vector = csv.writer(csvfile_train)
                        writer_vector.writerow(inout_data)
                        pre_app_id = app_id
                        occtime = time
                        exectime = lis["apprunning_time"]
                        ip_count = lis["ip_count"]
                        app_name = lis["app_name"]
                        input_vector = self.get_input_vector(app_id, cur_occtime_levle_id, ip_count_level_id,
                                                     exec_time_level_id)
                    if all_get == 0 and not key == None:
                        es.update(index="lstm",doc_type="_doc",id=key,body = {"doc": { "status":"1" }})
                    try:
                        es.update(index="lstm", doc_type="_doc", id=key, body={"doc": {"status": "4"}})
                    except:
                        self.logger.info("there was no data to recover ")

                if cur_data_cnt <= 1:
                    cur_data_cnt = 0
                total_cur_data_cnt = total_cur_data_cnt+cur_data_cnt
                total_cur_data_cnt = total_cur_data_cnt - 1
                if total_cur_data_cnt < 0:
                    total_cur_data_cnt = 0
                self.logger.info("one data already access")
                if cur_data_cnt > learn_cnt:
                    break
                elif cur_data_cnt < cfg_limit:
                    break
                elif total_cur_data_cnt > learn_cnt:
                    break


            total_csv_row_cnt = 0
            if total_cur_data_cnt >= 0:
                total_csv_row_cnt = total_cur_data_cnt + pre_csv_row_cnt
            return total_csv_row_cnt, pre_csv_row_cnt, total_cur_data_cnt,learn_start_date,learn_start_posi

        except:
            if csvfile_train:
                csvfile_train.close()
            self.logger.error("es scan error.")
            self.logger.error(traceback.format_exc())
            return 0,0,0,None,None

    # def get_data_from_hbase(self,learn_start_date,learn_start_posi,learn_cnt,all_get):
    #     self.logger.info("get data from hbase")
    #     # 初始化输出文件
    #     path_train = self.client_cfg.get_value("train_data") + '/' + str(self.active_uid.value,'utf-8') + '/'
    #     if not os.path.exists(path_train):
    #         os.makedirs(path_train)
    #
    #     csvfile_train = None
    #     try:
    #         path_train += "train_data"
    #         csvfile_train = open(path_train, 'a+', newline='')
    #         writer_vector = csv.writer(csvfile_train)
    #
    #     except:
    #         if csvfile_train:
    #             csvfile_train.close()
    #
    #         self.logger.error("save csv vector data error.")
    #         self.logger.error(traceback.format_exc())
    #         return 0,0,None,None,None
    #
    #     try:
    #         h_base_host = self.client_cfg.get_value("hbase_url")
    #         h_base_port = self.client_cfg.get_value("hbase_port")
    #         connection = happybase.Connection(host=h_base_host, port=h_base_port,
    #                                           timeout=None, autoconnect=True,
    #                                           table_prefix=None,
    #                                           table_prefix_separator=b'_',
    #                                           compat='0.98',
    #                                           transport='buffered', protocol='binary')
    #         connection.open()
    #     except:
    #         self.logger.error("hbase connect error.")
    #         self.logger.error(traceback.format_exc())
    #         return 0,None,None
    #     # 从配置文件中获取最大支持App数量
    #
    #     #vector_len=200
    #     # 获取当前数据数量（数据文件行数）
    #     pre_csv_row_cnt = 0
    #     try:
    #         pre_csv_row_cnt = len(open(path_train,"r").readlines())
    #     except:
    #         pass
    #
    #     try:
    #         table = connection.table('macdlp_app')
    #         b_uid = self.active_uid.value
    #
    #         filter_str = None
    #         row_start_str = None
    #         row_prefix_str = None
    #         # 全量学习,增量学习 ( 数据范围检索条件：起始行 + 长度 ）
    #         if all_get == 1:
    #             row_start_str = learn_start_posi
    #         else: #初始学习 ( 数据范围检索条件： 前缀 + 标记 + 长度 ）
    #             filter_str = "SingleColumnValueFilter('af','status',=,'binary:0')"
    #             row_prefix_str = b_uid
    #         cfg_limit = None
    #         self.logger.info("get data uid"+"["+str(b_uid)+"]")
    #         try:
    #             if self.client_cfg.get_value('data_extract_len'):
    #                 cfg_limit = int(self.client_cfg.get_value('data_extract_len'))
    #         except:
    #             self.logger.error("configure error[data_extract_len].")
    #         total_cur_data_cnt = 0
    #         while True:
    #             #特殊场景抽取2条数据
    #             if cfg_limit <= 1:
    #                 cfg_limit = 2
    #             # 获取数据扫描器，过滤条件为标签为0
    #
    #             scanner = table.scan(row_start=row_start_str,
    #                                  row_stop=None,
    #                                  columns=['af'],
    #                                  row_prefix=row_prefix_str,
    #                                  limit=cfg_limit,#20
    #                                  filter=filter_str)
    #
    #             cur_data_cnt = 0
    #             pre_app_id = None
    #             occtime= None
    #             exectime=None
    #             ip_count=None
    #             app_name=None
    #             pre_key=None
    #             input_vector=None
    #             information_vector=None
    #             inout_data=None
    #
    #             for key, data in scanner:
    #                 cur_data_cnt+=1
    #                 timestamp = key.split(bytes('_', 'utf-8'), 3)
    #                 if learn_start_posi == None or learn_start_posi=='':
    #                     # rowkey
    #                     learn_start_posi = key
    #                     learn_start_date = timestamp[3]
    #                 for k, v in data.items():
    #                     #['2018-07-27 09:00:42', '5', '1', 'com.tencent.xinWeChat', '2']
    #                     data_list = []
    #                     if str(k, 'utf-8') == 'af:name':
    #                         data_iter = data.values()
    #                         #time
    #                         data_list.append(str(timestamp[3], 'utf-8'))
    #                         for v in data_iter:
    #                             data_list.append(str(v, 'utf-8'))
    #                         #dura_time level
    #                         if pre_app_id == None:
    #                             exec_time_level_id = self.get_exectime_level_id(data_list[1])
    #                             cur_occtime_levle_id=self.get_occtime_level_id(data_list[0])
    #                             ip_count_level_id=self.get_ipcount_level_id(data_list[2])
    #                             app_id=self.get_app_id(data_list[3])
    #                             information_lable = self.get_information_lable(app_id, cur_occtime_levle_id,
    #                                                                            ip_count_level_id, exec_time_level_id)
    #                             information_id = self.get_information_id(information_lable)
    #                             input_vector=self.get_input_vector(app_id,cur_occtime_levle_id,ip_count_level_id,exec_time_level_id)
    #                             pre_app_id = app_id
    #                             occtime = data_list[0]
    #                             exectime = data_list[1]
    #                             ip_count=data_list[2]
    #                             app_name=data_list[3]
    #                         else:
    #                             exec_time_level_id = self.get_exectime_level_id(data_list[1])
    #                             cur_occtime_levle_id = self.get_occtime_level_id(data_list[0])
    #                             ip_count_level_id = self.get_ipcount_level_id(data_list[2])
    #                             app_id = self.get_app_id(data_list[3])
    #                             information_lable=self.get_information_lable(app_id,cur_occtime_levle_id,ip_count_level_id,exec_time_level_id)
    #                             information_id=self.get_information_id(information_lable)
    #                             information_vector=self.get_information_vector(information_id)
    #                             inout_data=input_vector+information_vector+[app_name]+[occtime]+[ip_count]+[exectime]
    #                             writer_vector.writerow(inout_data)
    #                             pre_app_id = app_id
    #                             occtime = data_list[0]
    #                             exectime = data_list[1]
    #                             ip_count = data_list[2]
    #                             app_name = data_list[3]
    #                             input_vector = self.get_input_vector(app_id,cur_occtime_levle_id,ip_count_level_id,exec_time_level_id)
    #                 pre_key=key
    #                             #[00000x00000ssss]
    #                 # 设置处理完成标记,需要两条以上数据
    #                 if all_get == 0 and not pre_key == None :
    #                     table.put(pre_key, {'af:status': bytes('1', 'utf-8')})
    #             try:
    #                 table.put(pre_key,{'af:status':bytes('0','utf-8')})
    #             except:
    #                 self.logger.info("there was no data to recover ")
    #             # 最后一块数据读取完成
    #             #learn_cnt差额
    #             if cur_data_cnt<=1:
    #                 cur_data_cnt=0
    #             total_cur_data_cnt += cur_data_cnt
    #             total_cur_data_cnt = total_cur_data_cnt - 1
    #             if total_cur_data_cnt<0:
    #                 total_cur_data_cnt=0
    #
    #             if  cur_data_cnt > learn_cnt :
    #                 break
    #             elif cur_data_cnt <cfg_limit:
    #                 break
    #             elif total_cur_data_cnt>learn_cnt:
    #                 break
    #
    #
    #         #self.logger.info("hbase data count:" + str(total_cur_data_cnt))
    #         csvfile_train.close()
    #
    #         connection.close()
    #         # 只有一条可处理数据时，不提取
    #         total_csv_row_cnt=0
    #         if total_cur_data_cnt >= 0:
    #             total_csv_row_cnt = total_cur_data_cnt+pre_csv_row_cnt
    #         return total_csv_row_cnt,pre_csv_row_cnt,total_cur_data_cnt,learn_start_date,learn_start_posi
    #
    #     except:
    #         if csvfile_train:
    #             csvfile_train.close()
    #
    #         connection.close()
    #         self.logger.error("hbase scan error.")
    #         self.logger.error(traceback.format_exc())
    #         return 0,0,0,None,None

    def get_incr_learned_data_from_es(self,cur_id):
        path_train = self.client_cfg.get_value("train_data") + '/' + cur_id + '/'
        if not os.path.exists(path_train):
            os.makedirs(path_train)


        check_mode = self.client_cfg.get_value("check_mode")

        csvfile_train = None
        try:
            path_train += "train_data"
            csvfile_train = open(path_train, 'a+', newline='')
            writer_vector = csv.writer(csvfile_train)

        except:
            if csvfile_train:
                csvfile_train.close()
            # if csvfile:
            #     csvfile.close()
            self.logger.error("save csv vector data error.")
            self.logger.error(traceback.format_exc())
            return 0,None,None
        try:
            es_host = self.client_cfg.get_value("es_host")
            es_port = self.client_cfg.get_value("es_port")
            es = Elasticsearch([{
                "host": es_host,
                "port": es_port
            }])
            self.logger.info("es connetc sucess")
        except:
            self.logger.error("es connect error.")
            self.logger.error(traceback.format_exc())
            return 0,None,None
            # 从配置文件中获取最大支持App数量
        vector_len = self.client_cfg.get_value("app_count")
            # 获取当前数据数量（数据文件行数）
        csv_row_cnt = 0
        try:
            csv_row_cnt = len(open(path_train, "r").readlines())
        except:
            pass

        try:
            body = {

                "query": {
                    "bool": {
                        "must": [
                            {"match": {"status": "3"}},
                            {"match": {"tid": cur_id}}
                        ]
                    }
                }
            }
            result = es.search(index="lstm", body=body)
            limit = result["hits"]["total"]
            body = {
                "query": {
                    "bool": {
                            "must":[
                                {"match":{"status": "3"}},
                                {"match":{"tid":cur_id}}
                            ]
                    }
                },
                "size":limit
            }

            cur_data_cnt = 0
            pre_app_id = None
            occtime = None
            exectime = None
            ip_count = None
            app_name = None
            pre_key = None

            input_vector = None
            information_vector = None
            inout_data = None
            result = es.search(index="lstm", body=body)
            self.logger.info(" start get data from es")
            for list in result["hits"]["hits"]:

                self.logger.info("start access es data ")
                key = list["_id"]
                cur_data_cnt += 1
                lis = list["_source"]
                time = lis["occ_time"]
                time = time.replace('T', ' ')[0:19]
                if pre_app_id == None:
                    exec_time_level_id = self.get_exectime_level_id(lis["apprunning_time"])
                    cur_occtime_levle_id = self.get_occtime_level_id(time)
                    ip_count_level_id = self.get_ipcount_level_id(lis["ip_count"])
                    app_id = self.get_app_id(lis["app_name"])
                    information_lable = self.get_information_lable(app_id, cur_occtime_levle_id,
                                                                   ip_count_level_id, exec_time_level_id)
                    information_id = self.get_information_id(information_lable)
                    input_vector = self.get_input_vector(app_id, cur_occtime_levle_id, ip_count_level_id,
                                                         exec_time_level_id)
                    pre_app_id = app_id
                    occtime = time
                    exectime = lis["apprunning_time"]
                    ip_count = lis["ip_count"]
                    app_name = lis["app_name"]
                else:
                    exec_time_level_id = self.get_exectime_level_id(lis["apprunning_time"])
                    cur_occtime_levle_id = self.get_occtime_level_id(time)
                    ip_count_level_id = self.get_ipcount_level_id(lis["ip_count"])
                    app_id = self.get_app_id(lis["app_name"])
                    information_lable = self.get_information_lable(app_id, cur_occtime_levle_id, ip_count_level_id,
                                                                   exec_time_level_id)
                    information_id = self.get_information_id(information_lable)
                    information_vector = self.get_information_vector(information_id)
                    inout_data = input_vector + information_vector + [app_name] + [occtime] + [ip_count] + [exectime]
                    writer_vector.writerow(inout_data)
                    pre_app_id = app_id
                    occtime = time
                    exectime = lis["apprunning_time"]
                    ip_count = lis["ip_count"]
                    app_name = lis["app_name"]
                    input_vector = self.get_input_vector(app_id, cur_occtime_levle_id, ip_count_level_id,
                                                         exec_time_level_id)
                es.update(index="lstm", doc_type="_doc", id=key, body={"doc": {"status": "1"}})
            read_data_cnt = cur_data_cnt

            csvfile_train.close()
            if check_mode == 1:
            #只有一条可处理数据时，不提取
                csv_row_data_cnt = 0
            if read_data_cnt > 1:
                csv_row_data_cnt = read_data_cnt + csv_row_cnt
            return csv_row_data_cnt

        except:
            if csvfile_train:
                csvfile_train.close()
            self.logger.error("es scan error.")
            self.logger.error(traceback.format_exc())
            return 0

    # def get_incr_learned_data_from_hbase(self,cur_uid):
    #
    #     # 初始化输出文件
    #     path_train = self.client_cfg.get_value("train_data") + '/' + str(cur_uid) + '/'
    #     if not os.path.exists(path_train):
    #         os.makedirs(path_train)
    #
    #
    #     check_mode = self.client_cfg.get_value("check_mode")
    #
    #     csvfile_train = None
    #     try:
    #         path_train += "train_data"
    #         csvfile_train = open(path_train, 'a+', newline='')
    #         writer_vector = csv.writer(csvfile_train)
    #
    #     except:
    #         if csvfile_train:
    #             csvfile_train.close()
    #         # if csvfile:
    #         #     csvfile.close()
    #         self.logger.error("save csv vector data error.")
    #         self.logger.error(traceback.format_exc())
    #         return 0,None,None
    #
    #     try:
    #         h_base_host = self.client_cfg.get_value("hbase_url")
    #         h_base_port = self.client_cfg.get_value("hbase_port")
    #         connection = happybase.Connection(host=h_base_host, port=h_base_port,
    #                                           timeout=None, autoconnect=True,
    #                                           table_prefix=None,
    #                                           table_prefix_separator=b'_',
    #                                           compat='0.98',
    #                                           transport='buffered', protocol='binary')
    #         connection.open()
    #     except:
    #         self.logger.error("hbase connect error.")
    #         self.logger.error(traceback.format_exc())
    #         return 0,None,None
    #
    #     # 从配置文件中获取最大支持App数量
    #     vector_len=self.client_cfg.get_value("app_count")
    #     # 获取当前数据数量（数据文件行数）
    #     csv_row_cnt = 0
    #     try:
    #         csv_row_cnt = len(open(path_train,"r").readlines())
    #     except:
    #         pass
    #
    #     try:
    #         table = connection.table('macdlp_app')
    #
    #         row_prefix_str = bytes(cur_uid,'utf-8')
    #
    #         read_data_cnt = 0
    #         # 获取数据扫描器，过滤条件为标签为3
    #         scanner = table.scan(row_start=None, row_stop=None, row_prefix=row_prefix_str,
    #                              columns=['af'], filter="SingleColumnValueFilter('af','status',=,'substring:3')")
    #
    #         cur_data_cnt = 0
    #         pre_app_id = None
    #         occtime = None
    #         exectime = None
    #         ip_count = None
    #         app_name = None
    #         pre_key = None
    #
    #         input_vector = None
    #         information_vector = None
    #         inout_data = None
    #         for key, data in scanner:
    #             cur_data_cnt += 1
    #             timestamp = key.split(bytes('_', 'utf-8'), 3)
    #
    #             for k, v in data.items():
    #                 # ['2018-07-27 09:00:42', '5', '1', 'com.tencent.xinWeChat', '2']
    #                 data_list = []
    #                 if str(k, 'utf-8') == 'af:name':
    #                     data_iter = data.values()
    #                     # time
    #                     data_list.append(str(timestamp[3], 'utf-8'))
    #                     for v in data_iter:
    #                         data_list.append(str(v, 'utf-8'))
    #                     # dura_time level
    #                     if pre_app_id == None:
    #                         exec_time_level_id = self.get_exectime_level_id(data_list[1])
    #                         cur_occtime_levle_id = self.get_occtime_level_id(data_list[0])
    #                         ip_count_level_id = self.get_ipcount_level_id(data_list[2])
    #                         app_id = self.get_app_id(data_list[3])
    #                         information_lable = self.get_information_lable(app_id, cur_occtime_levle_id,
    #                                                                        ip_count_level_id, exec_time_level_id)
    #                         information_id = self.get_information_id(information_lable)
    #                         input_vector = self.get_input_vector(app_id, cur_occtime_levle_id,
    #                                                              ip_count_level_id, exec_time_level_id)
    #                         pre_app_id = app_id
    #                         occtime = data_list[0]
    #                         exectime = data_list[1]
    #                         ip_count = data_list[2]
    #                         app_name = data_list[3]
    #                     else:
    #                         exec_time_level_id = self.get_exectime_level_id(data_list[1])
    #                         cur_occtime_levle_id = self.get_occtime_level_id(data_list[0])
    #                         ip_count_level_id = self.get_ipcount_level_id(data_list[2])
    #                         app_id = self.get_app_id(data_list[3])
    #                         information_lable = self.get_information_lable(app_id, cur_occtime_levle_id,
    #                                                                        ip_count_level_id, exec_time_level_id)
    #                         information_id = self.get_information_id(information_lable)
    #                         information_vector = self.get_information_vector(information_id)
    #                         inout_data = input_vector + information_vector + [app_name] + [occtime] + [ip_count] + [
    #                             exectime]
    #                         writer_vector.writerow(inout_data)
    #                         pre_app_id = app_id
    #                         occtime = data_list[0]
    #                         exectime = data_list[1]
    #                         ip_count = data_list[2]
    #                         app_name = data_list[3]
    #                         input_vector = self.get_input_vector( app_id, cur_occtime_levle_id,
    #                                                              ip_count_level_id, exec_time_level_id)
    #             pre_key = key
    #             table.put(pre_key, {'af:status': bytes('1', 'utf-8')})
    #         #self.logger.info("hbase data count:" + str(cur_data_cnt-1))
    #         # 最后一块数据读取完成
    #         read_data_cnt = cur_data_cnt
    #
    #         csvfile_train.close()
    #         if check_mode == 1:
    #             csvfile_train.close()
    #         connection.close()
    #     # 只有一条可处理数据时，不提取
    #         csv_row_data_cnt = 0
    #         if read_data_cnt > 1:
    #             csv_row_data_cnt = read_data_cnt + csv_row_cnt
    #         return csv_row_data_cnt
    #
    #     except:
    #         if csvfile_train:
    #             csvfile_train.close()
    #
    #         connection.close()
    #         self.logger.error("hbase scan error.")
    #         self.logger.error(traceback.format_exc())
    #         return 0
    def get_incr_learn_uid(self):
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")

        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
        try:
            sql_txt='select uid,count(incr_learn) from lstm_prediction where incr_learn=3 group by uid'
            cur=conn.cursor()
            reQuery=cur.execute(sql_txt)
            if reQuery>0:
                resu=cur.fetchall()
                return resu

        except Exception:
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
            return None
    def set_total_learned_count(self,incr_cnt,cur_uid):
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")

        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
        try:
            learned_cnt=0
            cur = conn.cursor()
            sql_txt1="select learned_cnt from lstm_uid where uid='" + cur_uid + "'; "
            reQuery = cur.execute(sql_txt1)
            if reQuery > 0:
                resu = cur.fetchall()
                learned_cnt=resu[0][0]
            learned_cnt=learned_cnt+incr_cnt
            try:
                sql_txt2="UPDATE lstm_uid SET learned_cnt='"+str(learned_cnt)+"' WHERE uid='" + cur_uid + "';"
                reQuery2=cur.execute(sql_txt2)
                conn.commit()
            except:
                self.logger.error("execute sql failed")
        except Exception:
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()
    def set_incr_learned_finished(self,cur_uid):
        try:
            conn = self.mydbspool.connection()
            if not conn:
                self.logger.error("Manager database cannot connected.")

        except Exception:
            self.logger.error("Mysql connect error (" + self.client_cfg.get_value("mysql_host") + ").")
            self.logger.error(traceback.format_exc())
        try:
            cur = conn.cursor()
            sql_txt1="select count(incr_learn) from lstm_prediction where incr_learn=3 and uid='"+ cur_uid +"';"
            slq_requery=cur.execute(sql_txt1)
            sql_txt2 = "UPDATE lstm_prediction SET incr_learn=2 WHERE incr_learn=3 and uid='" + cur_uid + "';"
            if slq_requery>0:
                res=cur.fetchall()
                if res[0][0]>=self.client_cfg.get_value("incr_learn_min_count"):
                    cur.execute(sql_txt2)
                    conn.commit()
        except Exception:
            self.logger.error(traceback.format_exc())
            cur.close()
            conn.close()

    def run(self):
        self.logger = init_log(const.PROJECT_NAME + '.' + str(self.name))
        self.logger.info("=================================")
        self.logger.info("Start access log")
        self.client_cfg = client_service_json_conf(const.JSON_CFG_FILE_NAME)
        ##set signal handler
        signal.signal(signal.SIGTERM, my_signal_handler)
        signal.signal(signal.SIGINT, my_signal_handler)
        self.init_mysql()
        if not self.init_new_app_id_pro():
            return
        if not self.init_app_id_from_db():
            return
        if not self.init_new_information_id_pro():
            return
        if not self.init_information_id_from_db():
            return
        report_time = time.time()
        ## start main loop
        global client_access_run_flg

        while client_access_run_flg:
            # 收集本进程状态信息，发送到管理进程
            if self.info_queue:
                #if True:
                if (time.time() - report_time) > 300:
                    self.info_queue.put(self.make_status_data())
                    report_time = time.time()
            # 清空全局变量
            self.db_data_id = ''
            self.video_name_id = ''
            self.starttm = ''
            self.duration = ''
            self.new_uid_id = set()
            self.video_file_name = ''

            # 等待启动状态时，开始执行业务，否则睡眠等待
            while self.run_status.value != 1:
                self.logger.info("Process is idle. Wait to running. pid[" + str(os.getpid()) + "]")
                if not client_access_run_flg:
                    break

                time.sleep(10)

            #进程切换到工作状态后，检查系统工作状态是否需要停止
            if not client_access_run_flg:
                break

            # 设置工作状态
            self.run_status.value = 2
            self.logger.info("Process is to running. pid[" + str(os.getpid()) + "]")
            self.logger.info("uid = [" + str(self.active_uid.value) + "]")

            run_start = time.time()
            can_learn = False
            can_incr_learn=False
            get_learn_start_date, get_learn_start_posi, get_learned_cnt,get_learned_state = self.get_learn_progress()
            # self.logger.info("learned_cnt"+str(learned_cnt))
            lstm_max_learn_cnt = self.client_cfg.get_value("lstm_max_learn_cnt")
            # 学习完成后，根据实际需要调整模型，再增量学习
            learned_state = self.get_incr_learned_state()
            uid_cnt_iter=self.get_incr_learn_uid()
            cur_uid=str(self.active_uid.value,'utf-8')
            total_csv_row_cnt=0
            incr_cnt = 0
            cur_csv_data_lines=0
            path_train = self.client_cfg.get_value("train_data") + '/' + str(self.active_uid.value, 'utf-8') + '/train_data'

            if not os.path.exists(path_train):
                pass
            else:
                cur_csv_data_lines=len(open(path_train,'r').readlines())
            if not uid_cnt_iter is None:
                incr_uid=None
                for ele in uid_cnt_iter:
                    ele=list(ele)
                    incr_uid=ele[0]
                    incr_cnt=ele[1]
                if incr_uid == cur_uid and incr_cnt >= self.client_cfg.get_value("incr_learn_min_count"):
                    if learned_state == 10 or learned_state == 11:
                        incr_learn_read_cnt = self.get_incr_learned_data_from_es(cur_uid)
                        self.set_total_learned_count(incr_learn_read_cnt, cur_uid)
                        can_incr_learn = True
                        self.set_incr_learned_finished(cur_uid)
                        self.logger.info("Incr learn data read finished. learned_cnt=[" + str(incr_learn_read_cnt) + "]")

            # 前期增量学习完成，进行最终全量学习
            # elif get_learned_cnt >= lstm_max_learn_cnt and get_learned_state!=30:
            #     total_csv_row_cnt, pre_csv_row_cnt, cur_read_cnt, learn_start_date, learn_start_posi=self.get_data_from_es(get_learn_start_date,get_learn_start_posi,lstm_max_learn_cnt,1)
            #
            #     can_learn = True
            #     self.logger.info("All learn data read finished. learned_cnt=[" + str(total_csv_row_cnt) + "]")
            # #正常学习

            elif get_learned_state==10 and get_learned_cnt<lstm_max_learn_cnt or cur_csv_data_lines>=lstm_max_learn_cnt:
                total_csv_row_cnt,pre_csv_row_cnt,cur_read_cnt,learn_start_date,learn_start_posi = self.get_data_from_es(get_learn_start_date,
                                                    #'root_181614340_121372777253_1533035305',
                                                    get_learn_start_posi,
                                                    lstm_max_learn_cnt-get_learned_cnt,0)

                #学习许可条件：读取数据数量大于可学习条件 或 已经获取总数量达到完全学习数量时
                if cur_read_cnt >= self.client_cfg.get_value("lstm_learn_min_data") or total_csv_row_cnt>=lstm_max_learn_cnt:
                    can_learn = True

                if get_learn_start_posi is None or get_learn_start_posi=='':
                    self.set_learn_progress(learn_start_date,learn_start_posi,cur_read_cnt + get_learned_cnt)
                else:
                    self.set_learn_progress(get_learn_start_date,get_learn_start_posi,cur_read_cnt+get_learned_cnt)
                self.logger.info("Learn data read finished. learned_cnt=[" + str(cur_read_cnt + get_learned_cnt) + "]")
            run_end = time.time()
            self.logger.info("Read data finish. ( elapsed: %f s ) ", run_end - run_start)

            # 2. 学习数据
            if can_learn:
                #pass
                #self.train_lstm(1)
                self.logger.info("Train start.")
                run_start = time.time()

                ml = macdlp_lstm(str(self.active_uid.value,'utf-8'),self.client_cfg,self.logger)
                ml.init_lstm_param()
                if not ml.train_lstm():
                    pass
                else:
                    pass
                del ml

                # 学习完成，删除已学习数据
                # 全量学习完成，设置标记
                if total_csv_row_cnt >= lstm_max_learn_cnt:
                    self.set_learn_finished()

                    try:
                        csv_path = self.client_cfg.get_value("train_data") + '/' \
                                   + str(self.active_uid.value, 'utf-8') + '/train_data'

                        os.remove(csv_path)
                    except:
                        # pass
                        self.logger.error("file remove error.[" + csv_path + "]")
                        #self.logger.error("file remove error.[" + mx_path + "]")
            elif can_incr_learn:
                # pass
                # self.train_lstm(1)
                self.logger.info("Incr Train start.")
                run_start = time.time()
                ml = macdlp_lstm(str(self.active_uid.value, 'utf-8'), self.client_cfg, self.logger)
                ml.init_lstm_param()
                if not ml.train_lstm():
                    pass
                else:
                    pass
                del ml

                if incr_cnt >= self.client_cfg.get_value("incr_learn_min_count"):
                    try:
                        csv_path = self.client_cfg.get_value("train_data") + '/' \
                                   + str(self.active_uid.value, 'utf-8') + '/train_data'
                        os.remove(csv_path)
                    except:
                        # pass
                        self.logger.error("file remove error.[" + csv_path + "]")
                    self.set_learn_finished()

                run_end = time.time()
                self.logger.info("Incr Train data finish. ( elapsed: %f s ) ", run_end - run_start)            #
            if my_debug:
                self.run_status.value = 1
                self.logger.info("Simple debug mode ... pid[" + str(os.getpid()) + "]")
            else:
                self.run_status.value = 3
                self.logger.info("Process is to finished. pid[" + str(os.getpid()) + "]")

            continue

        self.logger.info("Main process stoped.")


my_debug =0
if my_debug:
    from multiprocessing import Queue,Value,Array
    run_status = Value('i',1)
    #active_uid = Value(c_char_p, b'abcdefg')
    active_uid = Array('c',256)
    active_uid.value=b'root_3232268588_185133326885868'
    vap = client_process(None,run_status,active_uid)
    vap.run()
