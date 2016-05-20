""" 签名及消息检查类
"""
# -*- coding: utf-8 -*-

import hmac
import hashlib
import json
# 操作命令类型定义

START_STOP = '0'
GET_INFO = '1'
GET_RECORD = '2'

# 数据库参数定义
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWD = ''
MYSQL_DB_NAME = 'esass'

# 日志地址定义
HOME_PATH = ''
LOG_PATH = HOME_PATH + '/logs'

# 全局返回码定义
SYS_BUSY = -1
REQ_SUCCESS = 0
MOSQ_VERSION_INVALID = 1
MOSQ_ILLEGAL_REQ = 2
MOSQ_TEMP_UNSERV = 3
MQSQ_ILLEGAL_IDENTIFY = 4
MOSQ_UNAUTH = 5
SIG_ERR = 4001
PARA_ILLEGAL = 4002
SYS_ERR = 6001

masters = [{"host": MYSQL_HOST,
            "user": MYSQL_USER,
            "passwd": MYSQL_PASSWORD,
            "db_name": MYSQL_DB_NAME,
            "port": MYSQL_PORT}]
slaves = [{"host": MYSQL_HOST,
           "user": MYSQL_USER,
           "passwd": MYSQL_PASSWORD,
           "db_name": MYSQL_DB_NAME,
           "port": MYSQL_PORT}]

global db_client = MysqlClient(masters, slaves, logger)
global g_log = init_logger(LOG_PATH + 'third.log')


class MsgChk(object):
    """ 定义消息检查类
    """

    def __init__(self):
        """
        """
        self._check_dict = {START_STOP: self. _start_check}
        self._err_rep_str = ''
        
    def _gen_sig(self, key='', data=None):
        """ 签名产生函数
        """
        if data is None:
            data = {}
        list = sorted(data.items(), key=lambda data: data[0])
        para_str = ''
        for item in list:
            if para_str == '':
                para_str = str(item[0]) + '=' + str[item[1]]
            else:
                para_str += '&' + str(item[0]) + '=' + str[item[1]]
        key += '&'

        return hmac.new(str(key), str(para_str), hashlib.sha1).digest().encode('base64').rstrip()
        
    def _validity_check(self, opr_type='', data=None):
        """ 检测数据内容合法性
        """
        if data is None:
            data = {}
        if not self._check_dict[opr_type](data):
            return False
        return True

    def _start_check(self, data):
        """ 检查启动参数
        """
        # 检查参数是否正确
        sid = data['session_id']
        pcode = data['pile_code']
        ino = data['inter_no']
        action = data['action']
        uid = data['user_id']
        vol = data['voltage']
        elect = data['elect']
        time = data['time']

        result = (len(pcode) != 16) and (ino != 1 or ino != 2) and (action != 1 or action != 2)
        if result is False:
            return False

        # 检查桩是否正在使用
        
        return True
    
    def _get_info_check(self, data):
        """
        """
        return True
    
    def _gen_err_reply(msg='', err_code=0):
        """ 请求错误,生成返回字符串
        """
        dict['ret'] = err_code
        dicto['msg'] = msg
        str = json.dumps(dict)
        return str

    def get_erro_str(self):
        """ 获取错误的回复字符串
        """
        return self._err_rep_str
    
    def do_check(self, msg, opr_type=''):
        """ 执行检查，默认检查内容
        """
        # 检查app_id是否合法
        id = msg['app_id']
        sql = 'select * from third_app_info where app_id=%s' % id
        ret = db_client.execute_query(sql)
        if ret is None:
            msg = 'app_id is not found in database,please check'
            g_log.info(msg)
            self._err_rep_str = self._gen_err_reply(msg, PARA_ILLEGAL)
            return False

        # 检查签名是否正确
        sql = 'select app_key from third_app_info where app_id =%s' % id
        ret = db_client.execute_query(sql)
        key = ret[0][0]
        sig = self._gen_sig(key, data)
        if msg['sig'] != sig:
            msg = 'data sig is erro,please check'
            g_log.info(msg)
            self._err_rep_str = self._gen_err_reply(msg, SIG_ERR)
            return False
            
        # 检查参数是否合法
        data = json.loads(msg['info'])
        if not self._validity_check(opr_type, data):
            msg = 'wrong parameter,please check'
            g_log.info(msg)
            self._err_rep_str = self._gen_err_reply(msg, PARA_ILLEGAL)
            return False

        return True
