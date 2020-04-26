#!/usr/bin/python
# -*- coding: utf-8 -*-

import time

import my_process.global_obj as global_obj


def thread_manager_uid_link():

    # 获取需要学习用户信息（执行一次）
    learning_data = global_obj.get_uid_from_db(global_obj.uid_status.learning)
    if not learning_data == None:
        for item in learning_data:
            u_id = item[0]
            u_uid = item[1]
            # 添加到管理队列
            global_obj.add_uid_to_list(u_uid)
            global_obj.add_uid_to_dict(u_id,u_uid,global_obj.uid_status.learning)

    while(global_obj.thr_running_flg):
        # 获取新增用户
        allot_data = global_obj.get_uid_from_db(global_obj.uid_status.alloted)
        if not allot_data == None:
            for item in allot_data:
                u_id = item[0]
                u_uid = item[1]

                if global_obj.set_status_to_db(u_id,global_obj.uid_status.learning):
                    # 添加到管理队列
                    global_obj.add_uid_to_list(u_uid)
                    global_obj.add_uid_to_dict(u_id, u_uid, global_obj.uid_status.learning)

        # 处理增量用户:设置状态到学习中，继续学习即可
        incr_learn_data = global_obj.get_uid_from_db(global_obj.uid_status.incr_learn)
        if not incr_learn_data == None:
            for item in incr_learn_data:
                u_id = item[0]
                u_uid = item[1]

                if global_obj.set_status_to_db(u_id, global_obj.uid_status.learning):
                    # 添加到管理队列
                    global_obj.add_uid_to_list(u_uid)
                    global_obj.add_uid_to_dict(u_id, u_uid, global_obj.uid_status.learning)

        # 处理重新学习用户
        relearn_data = global_obj.get_uid_from_db(global_obj.uid_status.relearn)
        if not relearn_data == None:
            for item in relearn_data:
                u_id = item[0]
                u_uid = item[1]

                #####
                # 1. 不管状态，删除学习模型  2.设置状态为学习中，开始学习
                #####
                if global_obj.set_status_to_db(u_id, global_obj.uid_status.learning):
                    # 添加到管理队列
                    global_obj.add_uid_to_list(u_uid)
                    global_obj.add_uid_to_dict(u_id, u_uid, global_obj.uid_status.learning)

        # 处理学习完成用户
        finished_data = global_obj.get_uid_from_db(global_obj.uid_status.finished)
        if not finished_data == None:
            for item in finished_data:
                u_id = item[0]
                u_uid = item[1]

                # 设置状态为停止状态，在计算管理线程中，移除出学习队列
                global_obj.set_state_to_dict(u_id,u_uid, global_obj.uid_status.finished)

        # 处理失效用户
        invalid_data = global_obj.get_uid_from_db(global_obj.uid_status.invalid)
        if not invalid_data == None:
            for item in invalid_data:
                u_id = item[0]
                u_uid = item[1]

                # 设置状态为停止状态，在计算管理线程中，移除出学习队列
                global_obj.set_state_to_dict(u_id,u_uid, global_obj.uid_status.invalid)

        #
        time.sleep(30)
def thread_manager_procee():
    while (global_obj.thr_running_flg):
        chk_list = global_obj.get_uid_link()
        #print("chk_list:"+str(chk_list))
        for cur_uid in chk_list:
            #print("cur_uid:"+str(cur_uid))
            uid_info = global_obj.get_uid_info_from_dict(cur_uid)
            if uid_info != None:
                #print("uid_info:"+str(uid_info))
                # 学习完成处理
                if uid_info["state"] == global_obj.uid_status.finished:
                    #print("finished")
                    # 设置为可检测状态
                    if global_obj.set_status_to_db(uid_info["id_key"], global_obj.uid_status.checked):
                        # 从学习队列中删除
                        global_obj.del_uid_from_list(cur_uid)
                        global_obj.del_uid_from_dict(cur_uid)
                    #print("delete cur_uid")
                # 失效用户处理
                elif uid_info["state"] == global_obj.uid_status.invalid:
                    #print("invalid")
                    # 设置为可检测状态
                    if global_obj.set_status_to_db(uid_info["id_key"], global_obj.uid_status.invalid_end):
                        # 从学习队列中删除
                        global_obj.del_uid_from_list(cur_uid)
                        global_obj.del_uid_from_dict(cur_uid)
                # 学习进程调度
                elif uid_info["state"] == global_obj.uid_status.learning:
                    # uid 未学习
                    #print("learning"+str(uid_info["is_running"]))
                    if not uid_info["is_running"] == 1:
                        #寻找空进程, 尝试10次未寻找到时，跳过此UID.根据配置文件执行

                        wait_cnt = 0
                        while wait_cnt < 10:
                            if not global_obj.thr_running_flg:
                                break
                            #print("thread manager procee: wait_cnt<10")
                            p = global_obj.get_idle_process()
                            # 无空进程时，睡眠等待
                            if p == None:
                                time.sleep(1)
                            else:
                                # 启动线程
                                #global_obj.set_running_state_to_dict(cur_uid,1)
                                uid_info["is_running"] = 1
                                global_obj.start_process(p,cur_uid)
                                break
                            # 无空闲进程时，一直等待。（当要处理数据大于处理进程数量时，保证数据能够全部被处理）
                            if global_obj.main_cfg.get_value("wait_idle") == 0:
                                wait_cnt += 1
                else:
                    global_obj.main_logger.error("Unknown state in uid_link. ")
                    # 删除？
            # 进程全部学习中时，睡眠后继续
            #print("process number:"+str(len(global_obj.get_uid_link())))
            if len(global_obj.get_uid_link()) <= global_obj.main_cfg.get_value("work_process"):
                time.sleep(10)
            # 空进程回收，执行状态恢复
            #print("thread_manager_procee outer while")
            p = global_obj.get_idle_process()
