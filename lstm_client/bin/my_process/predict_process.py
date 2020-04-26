#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import pytz
from DBUtils.PooledDB import PooledDB
import pymysql
import datetime

import json

import signal
import time
from   multiprocessing import Process,Queue
import json
import csv
import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch

from   util import const
from   conf_process.config_josn import client_service_json_conf
from   util.const_log import init_const
from   util.const_log import init_log

from   my_process.macdlp_lstm import macdlp_lstm

import traceback
import socket
import tensorflow as tf

local_host_name = socket.gethostname()

client_access_run_flg = True

# def my_signal_handler(self,signum, frame):
def my_signal_handler(signum, frame):
    global client_access_run_flg
    client_access_run_flg = False

class predict_process(Process):

    ### 鍏ㄥ眬鍙橀噺瀹氫箟 ##############
    client_cfg = None
    logger = None
    mydbspool = None

    global_app_id = dict()
    global_information_id=dict()
    ###########################################
    # 杩涚▼娴佺▼
    # 1: 鑾峰彇UID鏁版嵁   浠庢暟鎹簱涓幏鍙栧涔犲畬鎴愮姸鎬乁ID鍒楄〃
    # 2: UID寰幆3-5澶勭悊
    # 3:     鑾峰彇褰撳墠UID鏈娴嬫暟鎹?    # 4:     杈惧埌鍙娴嬫暟閲忔椂锛岃繘琛岀畻娉曟娴?    # 5:     淇濆瓨妫€娴嬬粨鏋滃埌鏁版嵁搴撶粨鏋滆〃
    # 6: 浠庢楠?寮€濮嬶紝閲嶆柊寰幆鎵ц
    ###########################################

    #璁板綍鏈鐞哢ID
    new_uid_id = set()
    ###鐘舵€佺粺璁?
    status_start_time=time.time()
    status_client_cnt=0
    status_uid_cnt = 0
    status_main_times = 0
    def __init__(self,arg_info_queue):
        super(predict_process, self).__init__()
        self.info_queue = arg_info_queue
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
    #
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
                    self.logger.error("Mysql create procedure (new_app_id) error.")
                    return False
            return True
    def get_app_id_from_db(self):

        conn = None
        #杩炴帴姹犱腑鑾峰彇杩炴帴
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
    # 鑾峰彇AppID锛屼笉瀛樺湪鏃跺垎閰?
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
    def get_app_id(self,app_name):
        app_id = -1
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
                    if app_id == -1:
                        pass
                self.global_app_id[app_name] = app_id
        except:
            self.logger.error("make app id error.")
        return app_id
    # 鑾峰彇鍙娴婾ID鏁版嵁
    def get_uid_data(self):
        all_uid_list = []
        conn = None
        # 杩炴帴姹犱腑鑾峰彇杩炴帴
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
                "SELECT uid FROM lstm_uid where client_host = '" + local_host_name + "' and state = " + str(30) + ";")
            if reCout > 0:
                for item in cur.fetchall():
                    all_uid_list.append(item[0])
        except Exception:
            self.logger.error("MySQL data read error.")
            self.logger.error(traceback.format_exc())
        cur.close()
        conn.close()
        return all_uid_list
    def get_exec_time_level(self,exec_time):
        if int(exec_time) <= 300:
            exec_time_level = 1
        elif 300 < int(exec_time) <= 600:
            exec_time_level = 2
        elif 600 < int(exec_time) <= 1200:
            exec_time_level = 3
        elif 1200 < int(exec_time) <= 2400:
            exec_time_level = 4
        elif 2400 < int(exec_time) <= 3600:
            exec_time_level = 5
        else:
            exec_time_level = 6
        return exec_time_level
    def get_exec_time_range(self,exec_time_level):
        if exec_time_level==1:
            exec_time_range='0-5'
        if exec_time_level==2:
            exec_time_range='5-10'
        if exec_time_level==3:
            exec_time_range='10-20'
        if exec_time_level==4:
            exec_time_range='20-40'
        if exec_time_level==5:
            exec_time_range='40-60'
        if exec_time_level==6:
            exec_time_range='60-600'
        return exec_time_range
    def init_new_information_id_pro(self):
        create_procedure='''
        CREATE PROCEDURE `new_information_id`(IN information_label_param varchar(128))
        BEGIN
              DECLARE p_sqlcode INT DEFAULT 0;
              DECLARE ret_information_id INT;
              DECLARE duplicate_key CONDITION FOR 1062;
              DECLARE CONTINUE HANDLE FOR duplicate_key;
              BEGIN 
                    SET p_sqlcode=1502;
              END;
              SELECT @max_information_id:=(ifnull(max(information_id,0)+1)) from global_information_id;
              insert into global_information_id(information_id,information_lable) values (@max_information,information_label_param);
              set ret_information_id=@max_information_id;
              if p_sqlcode <>0 then
                    select @information_id_tmp:=information_id from global_information_id where information_lable=information_lable_param;
                    set ret_information_id=@information_id_tmp;
              end if;
              select ret_information_id;
        END;
        '''
        conn=None
        try:
            conn=self.mydbspool.connection()
            if not conn:
                self.logger.error("No mysql connection.")
                return False
        except Exception:
            self.logger.error("Mysql database could not connect.")
            self.logger.error(traceback.format_exc())
            return False
        try:
            cur=conn.cursor()
            query_re=cur.execute(create_procedure)
            cur.close()
            conn.close()
        except pymysql.InternalError as e:
            cur.close()
            conn.close()
            if e.args[0]!=1034:
                self.logger.error("Mysql create procedure new_information_id() failed.")
                return False
        return True
    def get_information_id_from_db(self):
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
            query_re=cur.execute("call new_information_id('"+information_lable+"');")
            if query_re:
                iter_re=cur.fetchall()
                information_id=int(iter_re[0][0])
            cur.close()
            conn.commit()
            conn.close()

        except Exception:
            self.logger.error("Mysql read data error.")
            self.logger.error(traceback.format_exc())
            return information_id
        return information_id
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
    def get_time(self,timestamp):
        tz=pytz.timezone('Asia/Shanghai')
        t=datetime.datetime.fromtimestamp(int(timestamp),tz).strftime("%Y-%m-%d %H:%M:%S")
        return t

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
    def get_occtime(self,occtime_level_id):
        if occtime_level_id==201:
            hour_time='0-8'
        elif occtime_level_id==202:
            hour_time='8-10'
        elif occtime_level_id==203:
            hour_time='10-12'
        elif occtime_level_id==204:
            hour_time='12-14'
        elif occtime_level_id==205:
            hour_time='14-16'
        elif occtime_level_id==206:
            hour_time='16-18'
        elif occtime_level_id==207:
            hour_time='18-20'
        elif occtime_level_id==208:
            hour_time='20-23'
        else:
            hour_time='No prediction'
        return hour_time

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
    def get_ip_count(self,ipcount_level_id):
        if ipcount_level_id==209:
            ipcount='0-0'
        elif ipcount_level_id==210:
            ipcount='1-3'
        elif ipcount_level_id==211:
            ipcount='4-6'
        elif ipcount_level_id==212:
            ipcount='7-10'
        elif ipcount_level_id==213:
            ipcount='11-100'
        else:
            ipcount='No prediction'
        return ipcount

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
    def get_exectime(self,exectime_level_id):
        if exectime_level_id==214:
            exectime='0-29'
        elif exectime_level_id==215:
            exectime='30-599'
        elif exectime_level_id==216:
            exectime='600-1799'
        elif exectime_level_id==217:
            exectime='1800-3599'
        elif exectime_level_id==218:
            exectime='3600-36000'
        else:
            exectime='No prediction'
        return exectime

    def get_information_lable(self,app_id, occtime_level_id, ipcount_level_id, exectime_level_id):
        information_lable = str(app_id) +'-'+ str(occtime_level_id) +'-'+ str(ipcount_level_id) +'-'+ str(exectime_level_id)
        return information_lable

    # def get_information_matrix(self,appid, occtime_level_id, ipcount_level_id, exectime_level_id):
    #     information_matrix = np.zeros((4, 200), dtype=np.float32)
    #     information_matrix[0, appid] = float(1)
    #     information_matrix[1, occtime_level_id] = float(1)
    #     information_matrix[2, ipcount_level_id] = float(1)
    #     information_matrix[3, exectime_level_id] = float(1)
    #     return information_matrix

    def get_information_vector(self,information_id):
        information_vector = np.zeros((1, 40000), dtype=np.float32)
        information_vector[0, information_id] = float(1)
        return list(information_vector[0])

    def get_information_lable_from_dict(self,information_id):
        try:
            id_lable={v:k for k,v in self.global_information_id.items()}
            information_lable=id_lable[information_id]
        except:
            information_lable=None
        return information_lable

    def get_appname_occtime_ipcount_exectime(self,information_lable):
        if  information_lable is not None:
            data_list=information_lable.split('-')
            appname=self.get_app_name(int(data_list[0]))
            occtime=self.get_occtime(int(data_list[1]))
            ipcount=self.get_ip_count(int(data_list[2]))
            exectime=self.get_exectime(int(data_list[3]))
            return appname,occtime,ipcount,exectime
        else:
            return 'No prediction','No prediction','No prediction','No prediction'
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
    def get_input_vector_2(self,vector_len,app_id,occtime_level_id,ipcount_level_id,exectime_level_id):
        matrix = np.zeros((1, vector_len ))
        matrix[:,app_id]=1
        matrix[:,occtime_level_id]=1
        matrix[:,ipcount_level_id]=1
        matrix[:,exectime_level_id]=1
        input_vector=list(matrix[0])
        return input_vector

    def get_learn_data_from_es(self, cur_uid, predict_min_cnt):
        # 鍒濆鍖栬緭鍑烘枃浠?
        path_predict = self.client_cfg.get_value("predict_data") + '/' + str(cur_uid)
        bk_pred_data = path_predict + '/bk_pred_data'
        path_pred = path_predict + '/pred_data'
        # original_data=path_predict+'/original_data.txt'
        # time_id_data = path_predict + '/time_id_data.txt'
        if not os.path.exists(path_predict):
            os.makedirs(path_predict)
        try:
            if os.path.exists(bk_pred_data):
                old_file = os.path.join(path_predict, 'bk_pred_data')
                new_file = os.path.join(path_predict, 'pred_data')
                os.rename(old_file, new_file)

        except:
            self.logger.error("rename file failed")
        csvfile_pred = open(path_pred, 'a', newline='')
        # ori_data=open(original_data,'a')
        # time_id_write=open(time_id_data,'a')
        writer_pred = csv.writer(csvfile_pred)
        csv_row_cnt = 0
        data_limit = 0
        try:
            csv_row_cnt = len(open(path_pred, "r").readlines())
            # self.logger.info("csv_row_cnt read" + str(csv_row_cnt))
            if csv_row_cnt <= 20 + self.client_cfg.get_value('lstm_time_steps'):
                data_limit = predict_min_cnt + self.client_cfg.get_value('lstm_time_steps') - csv_row_cnt - 1
            else:
                data_limit = 0
        except:
            pass
        try:
            es_host = self.client_cfg.get_value("es_host")
            es_port = self.client_cfg.get_value("es_port")
            es = Elasticsearch([{
                "host": es_host,
                "port": es_port
            }])
        except:
            self.logger.error("es connect error.")
            self.logger.error(traceback.format_exc())
            return 0, None, None
        vector_len = self.client_cfg.get_value("lstm_input_size")
        try:
            learn_start_posi = None
            learn_start_date = None
            body = {
                "sort": [{
                    "occ_time": {"order": "asc"}
                }],
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"status": "0"}},
                            {"match": {"tid": cur_uid}}
                        ]
                    }
                },
                "size": data_limit+1
            }
            result = es.search(index="lstm", body=body)
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
            happened_time = None
            key_list = []
            result_list = []
            for list in result["hits"]["hits"]:
                key=list["_id"]
                learn_start_posi=cur_uid
                cur_data_cnt += 1
                lis=list["_source"]
                time=lis["occ_time"]
                learn_start_date=time
                time = time.replace('T', ' ')[0:19]
                if pre_app_id == None:
                    exec_time_level_id = self.get_exectime_level_id(lis["apprunning_time"])
                    cur_occtime_levle_id = self.get_occtime_level_id(time)
                    ip_count_level_id = self.get_ipcount_level_id(lis["ip_count"])
                    app_id = self.get_app_id(lis["app_name"])
                    information_lable = self.get_information_lable(app_id, cur_occtime_levle_id,
                                                               ip_count_level_id, exec_time_level_id)
                    information_id = self.get_information_id(information_lable)
                    input_vector = self.get_input_vector_2(vector_len,app_id, cur_occtime_levle_id, ip_count_level_id,
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
                    information_lable = self.get_information_lable(app_id, cur_occtime_levle_id, ip_count_level_id,exec_time_level_id)
                    information_id = self.get_information_id(information_lable)
                    information_vector = self.get_information_vector(information_id)
                    inout_data = input_vector + information_vector + [app_name] + [occtime] + [ip_count] + [exectime]
                        #writer_vector.writerow(inout_data)
                    result_list.append(inout_data)
                    pre_app_id = app_id
                    occtime = time
                    exectime = lis["apprunning_time"]
                    ip_count = lis["ip_count"]
                    app_name = lis["app_name"]
                    input_vector = self.get_input_vector_2(vector_len,app_id, cur_occtime_levle_id, ip_count_level_id,
                                                         exec_time_level_id)
                key_list.append(key)

            if len(result_list) >= 1 and cur_data_cnt >= 2:
                for res in result_list:
                    writer_pred.writerow(res)
                        # for ti in da_li:
                        #     time_id_write.write(str(ti)+'\n')
                for k in range(len(key_list) - 1):
                        es.update(index="lstm", doc_type="_doc", id=key_list[k], body={"doc": {"status": "2"}})
            if cur_data_cnt <= 1:
                cur_data_cnt = 0
                read_data_cnt = cur_data_cnt
            else:
                read_data_cnt = cur_data_cnt - 1


            lstm_time_step = self.client_cfg.get_value("lstm_time_steps")
            pre_csv_row_cnt = read_data_cnt + csv_row_cnt - lstm_time_step
            if pre_csv_row_cnt <= 0:
                pre_csv_row_cnt = 0
                    # self.logger.info("csv_row_cnt"+str(pre_csv_row_cnt))
            return pre_csv_row_cnt, learn_start_posi, learn_start_date
        except:
                self.logger.error("es scan error.")
                self.logger.error(traceback.format_exc())
                return 0, None, None
        csvfile_pred.close()
        # ori_data.close()
        # time_id_write.close()




    # def get_data_from_hbase(self,cur_uid,predict_min_cnt):
    #     # 鍒濆鍖栬緭鍑烘枃浠?
    #     path_predict = self.client_cfg.get_value("predict_data") +'/'+str(cur_uid)
    #     bk_pred_data=path_predict+'/bk_pred_data'
    #     path_pred = path_predict + '/pred_data'
    #     #original_data=path_predict+'/original_data.txt'
    #     #time_id_data = path_predict + '/time_id_data.txt'
    #     if not os.path.exists(path_predict):
    #         os.makedirs(path_predict)
    #     try:
    #         if os.path.exists(bk_pred_data):
    #             old_file = os.path.join(path_predict, 'bk_pred_data')
    #             new_file = os.path.join(path_predict, 'pred_data')
    #             os.rename(old_file,new_file)
    #
    #     except:
    #         self.logger.error("rename file failed")
    #     csvfile_pred = open(path_pred, 'a', newline='')
    #     #ori_data=open(original_data,'a')
    #     #time_id_write=open(time_id_data,'a')
    #     writer_pred = csv.writer(csvfile_pred)
    #     csv_row_cnt = 0
    #     data_limit=0
    #     try:
    #         csv_row_cnt = len(open(path_pred, "r").readlines())
    #         #self.logger.info("csv_row_cnt read" + str(csv_row_cnt))
    #         if csv_row_cnt<=20+self.client_cfg.get_value('lstm_time_steps'):
    #             data_limit=predict_min_cnt+self.client_cfg.get_value('lstm_time_steps')-csv_row_cnt-1
    #         else:
    #             data_limit=0
    #     except:
    #         pass
    #     try:
    #         es = Elasticsearch([{
    #             "host": "192.168.6.130",
    #             "port": "9200"
    #         }])
    #     except:
    #         self.logger.error("hbase connect error.")
    #         self.logger.error(traceback.format_exc())
    #         return 0,None,None
    #     vector_len=self.client_cfg.get_value("lstm_input_size")
    #
    #     try:
    #         learn_start_posi = None
    #         learn_start_date = None
    #         table = connection.table('macdlp_app')
    #         filter_str = "SingleColumnValueFilter('af','status',=,'binary:0')"
    #         row_prefix_str = bytes(cur_uid,'utf-8')
    #         # scanner = table.scan(row_start=None,
    #         #                      row_stop=None,
    #         #                      columns=['af'],
    #         #                      row_prefix=row_prefix_str,
    #         #                      limit=data_limit+1,
    #         #                      filter=filter_str)
    #         cur_data_cnt = 0
    #         pre_app_id = None
    #         occtime = None
    #         exectime = None
    #         ip_count = None
    #         app_name = None
    #         pre_key = None
    #         input_vector = None
    #         information_vector = None
    #         inout_data = None
    #         happened_time=None
    #
    #         key_list=[]
    #         result_list=[]
    #         for key, data in scanner:
    #             cur_data_cnt += 1
    #             timestamp = key.split(bytes('_', 'utf-8'), 3)
    #
    #             if learn_start_posi == None:
    #                 # rowkey
    #                 learn_start_posi = key
    #                 learn_start_date = timestamp[3]
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
    #                         input_vector = self.get_input_vector(vector_len, app_id, cur_occtime_levle_id,
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
    #
    #                         result_list.append(inout_data)
    #                         pre_app_id = app_id
    #                         occtime = data_list[0]
    #                         exectime = data_list[1]
    #                         ip_count = data_list[2]
    #                         app_name = data_list[3]
    #                         input_vector = self.get_input_vector(vector_len, app_id, cur_occtime_levle_id,
    #                                                              ip_count_level_id, exec_time_level_id)
    #             key_list.append(key)
    #         if len(result_list)>=1 and cur_data_cnt>=2:
    #             for res in result_list:
    #                 writer_pred.writerow(res)
    #             # for ti in da_li:
    #             #     time_id_write.write(str(ti)+'\n')
    #             for k in range(len(key_list)-1):
    #                 table.put(key_list[k], {'af:status': bytes('2', 'utf-8')})
    #         if cur_data_cnt<=1:
    #             cur_data_cnt=0
    #             read_data_cnt=cur_data_cnt
    #         else:
    #             read_data_cnt =cur_data_cnt-1
    #         csvfile_pred.close()
    #         connection.close()
    #         lstm_time_step=self.client_cfg.get_value("lstm_time_steps")
    #         pre_csv_row_cnt = read_data_cnt+csv_row_cnt-lstm_time_step
    #         if pre_csv_row_cnt<=0:
    #             pre_csv_row_cnt=0
    #         #self.logger.info("csv_row_cnt"+str(pre_csv_row_cnt))
    #         return pre_csv_row_cnt,learn_start_posi,learn_start_date
    #     except:
    #
    #         connection.close()
    #         self.logger.error("hbase scan error.")
    #         self.logger.error(traceback.format_exc())
    #         return 0,None,None
    #     csvfile_pred.close()
    #     #ori_data.close()
    #     #time_id_write.close()

    def chk_create_lstm_prediction_table(self):
        conn = None;
        #杩炴帴姹犱腑鑾峰彇杩炴帴
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
            sql_txt = 'CREATE TABLE IF NOT EXISTS lstm_prediction( \
            	        id int auto_increment primary key, \
                        uid varchar(240) not NULL , \
                        happened_time datetime, \
                        happened_start_posi varchar(240), \
                        happened_end_posi varchar(240), \
                        original_data text , \
                        calc_data text,\
                        data_len int default 0,\
                        is_true int default 0,\
                        incr_learn int default 0,\
                        is_debug int default 0,\
                        oper_user varchar(240) default NULL ,\
                        oper_time datetime default NULL ) partition by hash(id) partitions 10;'
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
    def save_prediction_to_lstm_prediction_table(self,uid,happened_time,happened_start_posi,happened_end_posi,original_data,calc_data,data_len='0',is_true='0',incr_learn='0',is_debug='0',oper_user='0',oper_time='0'):
        conn = None;
        oper_time=datetime.datetime.fromtimestamp(0).strftime("%Y-%m-%d %H:%M:%S")

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
            sql_txt = "INSERT INTO lstm_prediction(uid,happened_time,happened_start_posi,happened_end_posi,original_data,calc_data,data_len,is_true,incr_learn,is_debug,oper_user,oper_time)\
                         values('"+uid+"','"+happened_time+"','"+happened_start_posi+"','"+happened_end_posi+"','"+original_data+"','"+calc_data+"','"+data_len+"','"+is_true+"','"+incr_learn+"','"+is_debug+"','"+oper_user+"','"+oper_time+"');"

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
    def get_app_name(self,app_id):
        app_name= None
        id_appname_dict = {v: k for k, v in self.global_app_id.items()}
        try:
            if app_id in id_appname_dict:
                app_name = id_appname_dict[app_id]
            else:
                # app id鎵惧埌鏃讹紝灏濊瘯閲嶆柊浠庢暟鎹簱鑾峰彇
                self.get_app_id_from_db()
                id_appname_dict = {v: k for k, v in self.global_app_id.items()}
                if app_id in id_appname_dict:
                    app_name = id_appname_dict[app_id]
                # 鏈鎵惧埌鏃讹紝鏀惧純姝ゆ暟鎹?
        except:
            self.logger.error("make app name error.")
        return app_name
    #def set_result_restore_signal(self,cur_uid,start_posi):

        #return bool_signal
    def run(self):
        self.logger = init_log(const.PROJECT_NAME + '.' + str(self.name))
        self.logger.info("=================================")
        self.logger.info("Start access log")
        self.client_cfg = client_service_json_conf(const.JSON_CFG_FILE_NAME)
        ##set signal handler
        signal.signal(signal.SIGTERM, my_signal_handler)
        signal.signal(signal.SIGINT, my_signal_handler)
        self.init_mysql()
        self.chk_create_lstm_prediction_table()
        if not self.get_app_id_from_db():
            return
        if not self.get_information_id_from_db():
            return
        report_time = time.time()
        ## start main loop
        global client_access_run_flg
        while client_access_run_flg:
            # 鏀堕泦鏈繘绋嬬姸鎬佷俊鎭紝鍙戦€佸埌绠＄悊杩涚▼
            if self.info_queue:
                #if True:
                if (time.time() - report_time) > 300:
                    self.info_queue.put(self.make_status_data())
                    report_time = time.time()
            # 娓呯┖鍏ㄥ眬鍙橀噺
            self.db_data_id = ''
            self.video_name_id = ''
            self.starttm = ''
            self.duration = ''
            self.new_uid_id = set()
            self.video_file_name = ''
            all_uid_list = self.get_uid_data()
            for cur_uid in all_uid_list:
                if not client_access_run_flg:
                    break
                self.logger.info("Predict uid. pid[" + str(cur_uid) + "]")
                run_start = time.time()
                lstm_predict_min_data_cnt = self.client_cfg.get_value("lstm_min_predict_cnt")
                read_cnt,start_row_key,predict_date = \
                    self.get_learn_data_from_es(cur_uid,lstm_predict_min_data_cnt)
                self.logger.info("Predict data read finished. predict_cnt=[" + str(read_cnt) + "]")
                if read_cnt >= lstm_predict_min_data_cnt-1:
                    self.logger.info("Predict start.")
                    self.get_information_id_from_db()
                    run_start = time.time()
                    ml = macdlp_lstm(cur_uid,self.client_cfg,self.logger)
                    ml.init_lstm_param()
                    #result_list is []
                    #
                    pred_information_id,real_information_id,accuracy,happened_time,happened_start_posi,happened_end_posi = ml.prediction()

                    happened_time=datetime.datetime.strptime(happened_time,"%Y-%m-%d %H:%M:%S")

                        #  #cur_uid,occ_time,rowkey_posi,original_data,calc_data,data_len
                    real_json_list=[]
                    pred_json_list=[]

                    for i in range(len(pred_information_id)):
                        p_information_id=pred_information_id[i]["pre_info_id"]
                        occ_time=pred_information_id[i]["occ_time"]
                        r_information_id=real_information_id[i]["rea_info_id"]
                        p_information_lable=self.get_information_lable_from_dict(p_information_id)
                        r_information_lable=self.get_information_lable_from_dict(r_information_id)
                        pred_app_name,pred_occ_time,pred_ip_count,pred_exec_time=self.get_appname_occtime_ipcount_exectime(p_information_lable)
                        real_app_name,real_occ_time,real_ip_count,real_exec_time=self.get_appname_occtime_ipcount_exectime(r_information_lable)
                        pred_json_data={'occ_time':occ_time,'app_name':pred_app_name,'occ_time_range':pred_occ_time,'ip_count_range':pred_ip_count,'exec_time_range':pred_exec_time}
                        real_json_data={'occ_time':occ_time,'app_name':real_app_name,'occ_time_range':real_occ_time,'ip_count_range':real_ip_count,'exec_time_range':real_exec_time}
                        real_json_list.append(real_json_data)
                        pred_json_list.append(pred_json_data)
                    k=0

                    if len(pred_json_list)<len(real_json_list):
                        n=len(pred_json_list)
                    else:
                        n=len(real_json_list)
                    print('n:'+str(n))
                    for i in range(n):
                        if pred_json_list[i]['occ_time_range'] =='No prediction':
                            continue
                        else:
                            occ_time_list=str.split(pred_json_list[i]['occ_time_range'],'-')
                            l_occ_time=int(occ_time_list[0])
                            g_occ_time=int(occ_time_list[1])
                        if pred_json_list[i]['ip_count_range']=='No prediction':
                            continue
                        else:
                            ip_count_list=str.split(pred_json_list[i]['ip_count_range'],'-')
                            l_ip_count=int(ip_count_list[0])
                            g_ip_count=int(ip_count_list[1])
                        if pred_json_list[i]['exec_time_range']=='No prediction':
                            continue
                        else:
                            exec_time_list=str.split(pred_json_list[i]['exec_time_range'],'-')
                            l_exec_time=int(exec_time_list[0])
                            g_exec_time=int(exec_time_list[1])

                        occtime=datetime.datetime.strptime(real_json_list[i]['occ_time'][0], "%Y-%m-%d %H:%M:%S").strftime("%H")
                        if real_json_list[i]['ip_count_range']=='No prediction':
                            continue
                        else:
                            ipcount_li=str.split(real_json_list[i]['ip_count_range'],'-')
                            ipcount=(int(ipcount_li[0])+int(ipcount_li[1]))/2
                        if real_json_list[i]['exec_time_range']=='No prediction':
                            continue
                        else:
                            exectime_li=str.split(real_json_list[i]['exec_time_range'],'-')
                            exectime=(int(exectime_li[0])+int(exectime_li[1]))/2
                        if pred_json_list[i]['app_name']==real_json_list[i]['app_name'] and l_occ_time<=int(occtime)<=g_occ_time and \
                            l_ip_count<=ipcount<=g_ip_count and l_exec_time<=exectime<=g_exec_time:
                            k+=1
                    if k==0:
                        acc=0
                    else:
                        acc=k/n
                    if pred_information_id != None and 0 <= acc < self.client_cfg.get_value("check_min_acc"):
                        try:
                            self.save_prediction_to_lstm_prediction_table(cur_uid,str(happened_time),happened_start_posi,happened_end_posi,json.dumps(real_json_list),json.dumps(pred_json_list))
                            #self.logger.info("saved prediction")
                        except:
                            self.logger.error("saved prediction failed")
                        pass
                    elif acc>=self.client_cfg.get_value("check_min_acc"):
                        self.logger.info("no abnormal action")
                    else:
                        self.logger.error("Predict failed.[" + cur_uid + "] date:[" + str(predict_date) + "]")
                    del ml

                    # try:
                    #     csv_path = self.client_cfg.get_value("predict_data") + '/' + cur_uid + '/pred_data'
                    #     os.remove(csv_path)
                    # except:
                    #     self.logger.error("delete pred_data failed")


                    try:
                        csv_path=self.client_cfg.get_value("predict_data")+'/'+cur_uid+'/pred_data'
                        bk_csv_path = self.client_cfg.get_value("predict_data") + '/' + cur_uid + '/bk_pred_data'
                        f=open(csv_path)
                        df=pd.read_csv(f,header=None)
                        line_num=self.client_cfg.get_value("lstm_time_steps")-1
                        data=df.iloc[-line_num:,:].values
                        ff=open(bk_csv_path,'a',newline='')
                        csv_writer=csv.writer(ff)
                        for dt in data:
                            csv_writer.writerow(dt)
                        f.close()
                        ff.close()
                        os.remove(csv_path)
                    except:
                        self.logger.error("write bk_pred_data failed")
                run_end = time.time()
                self.logger.info("Predict data finish. ( elapsed: %f s ) ", run_end - run_start)

            time.sleep(5)
        self.logger.info("Main process stoped.")

my_debug = 0
# if my_debug:
#     vap = predict_process(None)
#     vap.run()
