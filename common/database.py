'''
Created on May 28, 2015

@author: sunmoonone
'''

import logging
import umysql  # @UnresolvedImport
DEBUG=False

log = logging.getLogger(__name__)

class Connection(object):
    '''auto re-connect at mysql timeout
    a wrapper class to umysql connection
    '''
 
    def __init__(self, host,port,user,pwd,db=''):
        self.host=host
        self.port=port
        self.user=user
        self.pwd=pwd
        self.db=db
        self.cn = umysql.Connection()
        self.connect()
 
    def use(self,db):
        self.query('use %s' % db)
        self.db=db
 
    def connect(self):
        if(not self.cn.is_connected()):
            self.cn.connect (self.host, self.port, self.user, self.pwd,self.db)
 
    def execute(self,sql,args=None):
        need_connect,retry =False, 2
        while retry > 0:
            try:
                if need_connect:
                    self.connect()
                    need_connect=False

                if args:
                    return self.cn.query(sql,args)
                else:
                    return self.cn.query(sql)

            except:
                if not self.cn.is_connected():
                    need_connect=True
                    retry -= 1
                    continue

                if args:
                    log.exception('execute %s args: %s error' % (sql,args))
                else:
                    log.exception('execute %s error' % sql)
                
                return None
 
    def query(self,sql,args=None):
        return self.execute(sql, args)
