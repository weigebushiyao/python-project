import os
import csv
import numpy as np
import tensorflow as tf
import pandas as pd
import traceback


class macdlp_lstm:

    client_cfg = None
    logger = None
    active_uid = None

    def __init__(self,uid,cli_cfg,log):
        tf.logging.set_verbosity(tf.logging.DEBUG)
        self.client_cfg = cli_cfg
        self.logger = log
        self.active_uid = uid


    def init_lstm_param(self):

        self.lstm_rnn_unit = self.client_cfg.get_value("lstm_rnn_unit")
        self.lstm_input_size = self.client_cfg.get_value("lstm_input_size")
        self.lstm_time_steps = self.client_cfg.get_value("lstm_time_steps")
        self.lstm_batch_size = self.client_cfg.get_value("lstm_batch_size")
        self.lstm_lr = self.client_cfg.get_value("lstm_lr")
        self.lstm_output=self.client_cfg.get_value('lstm_output')
        self.idx=self.lstm_input_size+self.lstm_output
        tf.reset_default_graph()

        self.weights = {
            'in': tf.Variable(tf.random_normal([self.lstm_input_size, self.lstm_rnn_unit])),
            'out': tf.Variable(tf.random_normal([self.lstm_rnn_unit, self.lstm_output]))
        }
        self.biases = {
            'in': tf.Variable(tf.constant(0.1, shape=[1, self.lstm_rnn_unit])),
            'out': tf.Variable(tf.constant(0.1, shape=[1, self.lstm_output]))
        }

    def get_train_data(self):

        self.logger.info("get_train_data start.[" + self.active_uid + "]")
        size = self.lstm_input_size+self.lstm_output+4

        path = self.client_cfg.get_value("train_data") + '/' + self.active_uid +'/'
        try:
            f = open(path + "train_data")
            df = pd.read_csv(f)
        except:
            self.logger.error("file open is failured.[" + path  + "]")
            return None,None,None,None

        data_test = df.iloc[:, 0:size ].values
        idx=self.idx
        batch_index = []
        test_x, test_y, app_name,occ_time,ip_count,exec_time =[], [], [], [],[],[]
        for i in range(len(data_test) - self.lstm_time_steps):
            if i % self.lstm_batch_size == 0:
                batch_index.append(i)
            x = data_test[i:i + self.lstm_time_steps, :self.lstm_input_size]
            y = data_test[i:i + self.lstm_time_steps, self.lstm_input_size:idx]  # 验证集，为n维的数组
            appname = data_test[i:i + self.lstm_time_steps, idx:idx+1]
            occtime=data_test[i:i+self.lstm_time_steps,idx+1:idx+2]
            ipcount=data_test[i:i+self.lstm_time_steps,idx+2:idx+3]
            exectime=data_test[i:i+self.lstm_time_steps,idx+3]
            test_x.append(x.tolist())
            test_y.append(y.tolist())
            app_name.append(appname.tolist())
            occ_time.append(occtime.tolist())
            ip_count.append(ipcount.tolist())
            exec_time.append(exectime.tolist())
        batch_index.append((len(data_test) - self.lstm_time_steps))
        self.logger.info("get_train_data is End.[" + self.active_uid + "] data_count=" + str(len(app_name)) )
        return batch_index, test_x, test_y, app_name,occ_time,ip_count,exec_time


    # ——————————————————定义神经网络变量——————————————————
    def lstm(self,X):
        batch_size = tf.shape(X)[0]
        time_step = tf.shape(X)[1]
        w_in = self.weights['in']
        b_in = self.biases['in']
        input = tf.reshape(X, [-1, self.lstm_input_size])  # 需要将tensor转成2维进行计算，计算后的结果作为隐藏层的输入(batch_size*time_step,input_size)

        input_rnn = tf.matmul(input, w_in) + b_in  # (batch_size*time_step,rnn_unit)
        input_rnn = tf.reshape(input_rnn,
                               [-1, time_step, self.lstm_rnn_unit])  # (batch_size,time_step,rnn_unit) 将tensor转成3维，作为lstm cell的输入
        single_cell = tf.nn.rnn_cell.BasicLSTMCell(num_units=self.lstm_rnn_unit, reuse=tf.AUTO_REUSE)
        #single_cell = tf.nn.rnn_cell.BasicLSTMCell(num_units=self.lstm_rnn_unit)
        cell = tf.nn.rnn_cell.MultiRNNCell([single_cell] * 2)
        init_state = cell.zero_state(batch_size, dtype=tf.float32)  # (batch_size, state_size)
        output_rnn, final_states = tf.nn.dynamic_rnn(cell, input_rnn, initial_state=init_state,
                                                     dtype=tf.float32)  # output_rnn是记录lstm每个输出节点的结果，final_states是最后一个cell的结果

        output = tf.reshape(output_rnn, [-1, self.lstm_rnn_unit])  # 作为输出层的输入
        w_out = self.weights['out']
        b_out = self.biases['out']
        y = tf.matmul(output, w_out) + b_out
        pred = tf.nn.softmax(y)
        return pred, final_states
    def train_lstm(self):
        try:
            self.logger.info("train_lstm start.[" + self.active_uid  + "]")

            # init param
            lstm_X = tf.placeholder(tf.float32,
                                         shape=[self.lstm_batch_size, self.lstm_time_steps, self.lstm_input_size])
            lstm_Y = tf.placeholder(tf.float32,
                                         shape=[self.lstm_batch_size, self.lstm_time_steps, self.lstm_output])

            batch_index, train_x, train_y, time,_,_,_ = self.get_train_data()
            #训练数据获取失败时，结束处理
            if batch_index == None:
                return False
            lstm_pred, _ = self.lstm(lstm_X)
            lstm_loss = -tf.reduce_sum(tf.reshape(lstm_Y, [-1]) * tf.log(tf.clip_by_value(tf.reshape(lstm_pred, [-1]),1e-10,1)))

            #lstm_loss=tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=lstm_pred,labels=lstm_Y))
            #lstm_loss = tf.reduce_mean(tf.nn.sparse_softmax_cross_entropy_with_logits(labels=tf.reshape(lstm_Y, [-1]),
            #                                                                     logits=tf.reshape(lstm_pred,[-1])))
            #global_step = tf.train.get_or_create_global_step()
            #global_step=tf.Variable(0,trainable=False)
            #learning_rate = tf.train.exponential_decay(self.lstm_lr,global_step=global_step,decay_steps=50,decay_rate=0.95,staircase=False)
            #optimizer = tf.train.AdamOptimizer(learning_rate, beta1=0.5)
            #tvars = tf.trainable_variables()
            #grads, _ = tf.clip_by_global_norm(tf.gradients(lstm_cost, tvars), self.max_grad_norm)
            #optimizer = tf.train.AdamOptimizer(learning_rate=self.learning_rate)
            #lstm_train_op = optimizer.apply_gradients(zip(grads, tvars), global_step=tf.train.get_or_create_global_step())
            lstm_train_op = tf.train.AdamOptimizer(self.lstm_lr).minimize(lstm_loss)
            #opt_op = optimizer.minimize(loss_ctc, global_step=global_step)
            path_modle = self.client_cfg.get_value("modle_path") + '/' + self.active_uid + '/'
            if not os.path.exists(path_modle):
                os.makedirs(path_modle)
            # 保存最后一次训练的模型
            saver = tf.train.Saver(tf.global_variables(), max_to_keep=1, save_relative_paths=True)
            #path_modle_checkpoint = tf.train.latest_checkpoint(checkpoint_dir=path_modle)
            with tf.Session() as sess:
                last_loss=0
                for epo in range(self.client_cfg.get_value("train_epo")):
                    try:
                        sess.run(tf.global_variables_initializer())
                        # 参数恢复
                        saver.restore(sess, tf.train.latest_checkpoint(
                            path_modle))
                    except:
                        # 学习模型获取失败时，按照无模型冲洗开始学习
                        self.logger.error("restore modle failure.")
                    # 学习模型获取
                    flag=True
                    while flag: #and laern_cnt<max_learn_cnt:
                        for step in range(len(batch_index) - 2):
                            _, _loss = sess.run([lstm_train_op, lstm_loss], feed_dict={lstm_X: train_x[batch_index[step]:batch_index[step + 1]],
                                                                                             lstm_Y: train_y[batch_index[step]:batch_index[step + 1]]})

                        saver.save(sess, path_modle + "mod")
                        try:
                            self.logger.info("epo:"+str(epo)+'---'+"_loss:"+str(_loss))
                    #损失值
                            if abs(_loss)==0:
                                flag=False
                            if last_loss!=0:
                                if abs(last_loss-_loss)/last_loss < self.client_cfg.get_value("lstm_end_loss"):
                                    flag = False

                            last_loss=_loss
                        except:
                            self.logger.error("no _loss")
                            flag=False
                    epo+=1
            self.logger.info("train_lstm end.[" + self.active_uid + "]")
            return True
        except:
            self.logger.error("train_lstm is failured.")
            self.logger.error(traceback.format_exc())
            return False


    def get_predict_data(self):
        path = self.client_cfg.get_value("predict_data") + '/' + self.active_uid + '/pred_data'
        count = len(open(path,'rU').readlines())
        print("pred_data count:"+str(count))
        try:
              f = open(path)
              df = pd.read_csv(f,header=None)
        except:
              self.logger.error("file open is failed.[" + path + "]")
              return None,None,None,None,None

        size = self.lstm_input_size+self.lstm_output+4
        data_test = df.iloc[:, 0:size].values
        batch_index = []
        test_x, test_y, app_name, occ_time, ip_count, exec_time =[], [], [], [], [], []
        for i in range(len(data_test) - self.lstm_time_steps):
            if i % self.lstm_batch_size == 0:
                batch_index.append(i)
            x = data_test[i:i + self.lstm_time_steps, :self.lstm_input_size]
            y = data_test[i:i + self.lstm_time_steps, self.lstm_input_size:self.idx]  # 验证集，为n维的数组
            appname = data_test[i:i + self.lstm_time_steps, self.idx:self.idx+1]
            occtime = data_test[i:i + self.lstm_time_steps, self.idx+1:self.idx+2]
            ipcount = data_test[i:i + self.lstm_time_steps, self.idx+2:self.idx+3]
            exectime = data_test[i:i + self.lstm_time_steps, self.idx+3]
            #happened_time=data_test[i:i+self.lstm_time_steps,40201]
            test_x.append(x.tolist())
            test_y.append(y.tolist())
            app_name.append(appname.tolist())
            occ_time.append(occtime.tolist())
            ip_count.append(ipcount.tolist())
            exec_time.append(exectime.tolist())
        batch_index.append((len(data_test) - self.lstm_time_steps))
        return batch_index, test_x, test_y, app_name, occ_time, ip_count, exec_time

    def prediction(self):

        try:
            time_step = self.lstm_time_steps
            X = tf.placeholder(tf.float32, shape=[None, time_step, self.lstm_input_size])
            batch_index, test_x, test_y, app_name,occ_time,ip_count,exec_time = self.get_predict_data()
            pred, _ = self.lstm(X)
            saver = tf.train.Saver(tf.global_variables(), reshape=True)
            with tf.Session() as sess:
                sess.run(tf.global_variables_initializer())
                # 参数恢复
                saver.restore(sess, tf.train.latest_checkpoint(
                                        self.client_cfg.get_value("modle_path") + '/'
                                        + self.active_uid+ '/'))
                test_predict = []
                for step in range(len(test_x)):
                    prob = sess.run(pred, feed_dict={X: [test_x[step]]})
                    test_predict.append(prob.tolist())
                #print(test_predict)
                j=5
                pred_information_id=[]
                real_information_id=[]
                n=len(test_predict)
                k=0
                for i in range(n):
                    m=len(test_predict[i])
                    pr=test_predict[i][j].index(max(test_predict[i][j]))
                    pr_ret_dict={"occ_time":occ_time[i][0],"pre_info_id":pr}
                    re=test_y[i][j].index(max(test_y[i][j]))
                    re_ret_dict={"occ_time":occ_time[i][0],"rea_info_id":re}
                    if pr==re:
                        k+=1
                    pred_information_id.append(pr_ret_dict)
                    real_information_id.append(re_ret_dict)
                pred_start_posi=self.active_uid+'_'+str(occ_time[0][0][0])
                pred_end_posi=self.active_uid+'_'+str(occ_time[n-2][j][0])
                pred_happened_time=occ_time[0][0][0]
                # correct_pred=tf.equal(tf.arg_max(test_predict,1),tf.arg_max(test_y,1))
                # accuracy=tf.reduce_mean(tf.cast(correct_pred,tf.float32))
                # accuracy=accuracy.eval()
                if n==0:
                    n+=1
                    accuracy=k/n
                else:
                    accuracy=k/n

                self.logger.info("prediction completed!")
            return pred_information_id,real_information_id,accuracy,pred_happened_time,pred_start_posi,pred_end_posi
        except:
            self.logger.error("prediction_lstm is failed.")
            self.logger.error(traceback.format_exc())
            return None,0
