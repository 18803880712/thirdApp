# -*- coding: utf-8 -*-

import hmac
import hashlib
import tornado.httpserver
import tornado.ioloop
import tornado.options
import time
from mysql import *
from tornado.options import define, options
from check import *

# 数据库参数定义
MYSQL_HOST = '127.0.0.1'
MYSQL_PORT = 3306
MYSQL_USER = 'root'
MYSQL_PASSWD = ''
MYSQL_DB_NAME = 'esass'

# 日志地址定义
HOME_PATH = ''
LOG_PATH = HOME_PATH + '/logs'

# 第三方接口地址定义
OPR_URL = 'http://www.123.56.113.123/operate'
INFO_URL = 'http://www.123.56.113.123/info'
RECORD_URL = 'http://www.123.56.113.123/record'
BILL_URL = 'http://www.123.56.113.123/bill'
STATE_URL = 'http://www.123.56.113.123/state'

# 回调操作类型定义
PILE_OPR_CALLBACK = 1
PILE_STATE_CALLBACK = 2
PILE_RECORD_CALLBACK = 3
PILE_BILL_CALLBACK = 4

define('port', default=8000, help='监听8000端口', type=int)


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

logger = init_logger()
global db_client = MysqlClient(masters, slaves, logger) 
global g_log = init_logger(LOG_PATH + 'third.log')


class OprHandler(tornado.web.RequestHandler):
    """ 第三方启动停止处理类
    """
    def post(self):
        """ post 请求处理函数
        """
        dict = {'session_id': 0,
                'pile_code': '',
                'inter_no': 0,
                'action': 0,
                'user_id': 0,
                'voltage': 0,
                'elect': 0,
                'time': 0
                }
        str = json.dumps(dict)
        msg['app_id'] = self.get_argument('app_id', '')
        msg['info'] = self.get_argument('info', str)
        msg['sig'] = self.get_argument('sig', '')

        data = json.loads(msg['info'])
        msg_check = MsgChk()
        ret = msg_check.do_check(data, START_STOP)

        if ret is False:
            self.write(msg_check.get_erro_str())
        else:
            # 将操作写到device_opr 表中
            sql = 'INSERT INTO device_opr (app_id,session_id,pile_code, inter_no, action,state, create_time) VALUES(%s, %s, %s, %d, %d, 0, %d)' % (msg['app_id'], data['session_id'], data['pile_code'], data['inter_no'], data['action'], time.time())
            if -1 == db_client.execute_update(sql):
                g_log.info('执行sql:%s语句失败' % sql)

    
class InfoHandler(tornado.web.RequestHandler):
    """ 第三方召唤信息处理类
    """
    def post(self):
        """ post请求处理函数
        """
        msg = {}
        msg['app_id'] = self.get_argument('app_id', '')
        msg['info'] = self.get_argument('info', '')
        msg['sig'] = self.get_argument('sig', '')

        
class RecordHandler(tornado.web.RequestHandler):
    """ 第三方召唤实时数据处理类
    """
    def post(self):
        """ post请求处理函数
        """
        msg = {}
        msg['app_id'] = self.get_argument('app_id', '')
        msg['info'] = self.get_argument('info', '')
        msg['sig'] = self.get_argument('sig', '')

    
class BillHandler(tornado.web.RequestHandler):
    """ 第三方召唤账单处理类
    """
    def post(self):
        """ post请求处理函数
        """
        msg = {}
        msg['app_id'] = self.get_argument('app_id', '')
        msg['info'] = self.get_argument('info', '')
        msg['sig'] = self.get_argument('sig', '')

      
class StateHandler(tornado.web.RequestHandler):
    """ 第三方召唤状态处理类
    """
    def post(self):
        """ post请求处理函数
        """
        msg = {}
        msg['app_id'] = self.get_argument('app_id', '')
        msg['info'] = self.get_argument('info', '')
        msg['sig'] = self.get_argument('sig', '')

        
class ThirdServe(self):
    """ 第三方服务类
    """

    def __init__(self):
        """ 初始化函数
        """
        self._do_dict = {PILE_OPR_CALLBACK: self._do_opr_push,
                         PILE_BILL_CALLBACK: self._do_bill_push,
                         PILE_RECORD_CALLBACK: self._do_record_push,
                         PILE_STATE_CALLBACK: self._do_state_push}

        self._last_callback_id = 0
        
    def _post_data(self, data=None, url='', timeout=5):
        """ 向指定url post数据
        """
        if data is None:
            data = {}
        result = {}
        try:
            data = urllib.urlencode(data)
            resp = urllib2.urlopen(url, data, timeout).read()
            result = json.loads(resp)
        except Exception, e:
            print '发送失败'
            return

        return result
    
    def start(self):
        """ 开启数据推送线程
        """
        push_thread = threading.Thread(target=self._do push)
        push_thread.start()
        push_thread.join()

    def _do_bill_push(self, id, info):
        """ 账单推送
        """
        sql = 'select pile_bill_url from third_app_info where id = %s' % id
        rows = db_client.excute_query(sql)
        if rows is None:
            g_log.info('sql: %s语句执行失败' % sql)
            return False
        
        ret = self._post_data(rows, ret[0][0])
        if ret is None:
            g_log.info('推送数据失败')
            return False
        return True
            
    def _do_state_push(self, id, info):
        """ 状态推送
        """
        sql = 'select pile_state_url from third_app_info where id = %s and state = 1' % id
        rows = db_client.excute_query(sql)
        if rows is None:
            g_log.info('sql: %s语句执行失败' % sql)
            return False
        
        ret = self._post_data(rows, ret[0][0])
        if ret is None:
            g_log.info('推送数据失败')
            return False
        return True
       
    def _do_opr_push(self, id, info):
        """ 操作回调结果推送
        """
        sql = 'select opr_callback_url from third_app_info where id = %s and state = 1' % id
        rows = db_client.excute_query(sql)
        if rows is None:
            g_log.info('sql: %s语句执行失败' % sql)
            return False
        
        ret = self._post_data(rows, ret[0][0])
        if ret is None:
            g_log.info('推送数据失败')
            return False
        return True
    
    def _do_record_push(self, id, info):
        """ 实时数据推送
        """
        sql = 'select pile_record_url from third_app_info where id = %s and state = 1' % id
        rows = db_client.excute_query(sql)
        if rows is None:
            g_log.info('sql: %s语句执行失败' % sql)
            return False
        
        ret = self._post_data(rows, ret[0][0])
        if ret is None:
            g_log.info('推送数据失败')
            return False
        return True
        
    def _do_push(self):
        """ 检测表，推送数据
        """
        while True:
            sql = 'select id, type, state, data,proc_time, create_time from callback_log where state = 0 and id > %d ' % self._last_callback_id
            results = db_client.execute_query(sql)
            if results is None:
                g_log.error('Fail to excute sql:%s') % sql
            else:
                for ret in results:
                    id = ret[0]
                    dtype = int(ret[1])
                    state = ret[2]
                    info = json.loads(ret[3])
                    ptime = ret[4]
                    ctime = ret[5]

                    ret = self._do_dict[dtype](id, info)
                    if not ret:
                        g_log.error('回调执行失败')

                        ＃更新为已处理的状态
                        sql = 'update callback_log set state = 1 where app_id = %s' % id
                        db_client.execute_update(sql)
                        if int(id) > self._last_callback_id:
                            self._last_callback_id = id
                            # 删除已经处理过的数据
                        sql = 'delete from callback_log where id <= %s' % id
                        if -1 == db_client.execute_update(sql):
                            g_log.error('failed to remove callback_log:%s' % sql)
            sleep(1)
            
                
def main():
    """
    """
    third_serve = ThirdServe()
    tornado.options.parse_command_line()
    app = tornado.web.Application(handlers=[(r"/operate", OprHandler)])
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    third_serve.start()
    tornado.ioloop.IOLoop.instance().start()
    
if __name__ == '__main__':
    main()
