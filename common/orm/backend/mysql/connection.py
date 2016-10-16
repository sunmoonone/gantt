# -*- coding: utf-8 -*-
'''
Created on Oct 30, 2015

@author: sunmoonone
'''

import logging
import re
from mysql.connector import connection
from common.utils import ObjectDict

DEBUG=0
RE_WRITE_ST=re.compile('^insert|update|delete|create|drop|truncate.*',re.I)

class Connection(object):
    '''auto re-connect at mysql timeout
    a wrapper class to mysql-connector
    '''

    def __init__(self, host,port,user,password,database='',**kw):
        '''
        user (username*)        The user name used to authenticate with the MySQL server.
        password (passwd*)        The password to authenticate the user with the MySQL server.
        database (db*)        The database name to use when connecting with the MySQL server.
        host    127.0.0.1    The host name or IP address of the MySQL server.
        port    3306    The TCP/IP port of the MySQL server. Must be an integer.
        unix_socket        The location of the Unix socket file.
        auth_plugin        Authentication plugin to use. Added in 1.2.1.
        use_unicode    True    Whether to use Unicode.
        charset    utf8    Which MySQL character set to use.
        collation    utf8_general_ci    Which MySQL collation to use.
        autocommit    False    Whether to autocommit transactions.
        time_zone        Set the time_zone session variable at connection time.
        sql_mode        Set the sql_mode session variable at connection time.
        get_warnings    False    Whether to fetch warnings.
        raise_on_warnings    False    Whether to raise an exception on warnings.
        connection_timeout (connect_timeout*)        Timeout for the TCP and Unix socket connections.
        client_flags        MySQL client flags.
        buffered    False    Whether cursor objects fetch the results immediately after executing queries.
        raw    False    Whether MySQL results are returned as is, rather than converted to Python types.
        ssl_ca        File containing the SSL certificate authority.
        ssl_cert        File containing the SSL certificate file.
        ssl_key        File containing the SSL key.
        ssl_verify_cert    False    When set to True, checks the server certificate against the certificate file specified by the ssl_ca option. Any mismatch causes a ValueError exception.
        force_ipv6    False    When set to True, uses IPv6 when an address resolves to both IPv4 and IPv6. By default, IPv4 is used in such cases.
        dsn        Not supported (raises NotSupportedError when used).
        pool_name        Connection pool name. Added in 1.1.1.
        pool_size    5    Connection pool size. Added in 1.1.1.
        pool_reset_session    True    Whether to reset session variables when connection is returned to pool. Added in 1.1.5.
        compress    False    Whether to use compressed client/server protocol. Added in 1.1.2.
        converter_class        Converter class to use. Added in 1.1.2.
        fabric        MySQL Fabric connection arguments. Added in 1.2.0.
        failover        Server failover sequence. Added in 1.2.1.
        option_files        Which option files to read. Added in 2.0.0.
        option_groups    ['client', 'connector_python']    Which groups to read from option files. Added in 2.0.0.
        allow_local_infile    True    Whether to enable LOAD DATA LOCAL INFILE. Added in 2.0.0.
        use_pure    True    Whether to use pure Python or C Extension. Added in 2.1.1.
        '''
        self._config={
                      'host':host,
                      'port':int(port),
                      'user':user,
                      'password':password,
                      'database':database
                      }
        self._config.update(kw)
        self._autocommit=False
        if 'autocommit' in kw: 
            self._autocommit=kw['autocommit']
        
        self.cn = None
        self._cursorR=None
        self._cursorW=None
        self.connect()

    def use(self,db):
        '''
        @raise mysql.connector.Error 
        '''
        self.connect()
        self.cn.database = db
        self._config['database']=db

    def connect(self):
        '''
        @raise mysql.connector.Error 
        '''
        if not self.cn:
            self.cn = connection.MySQLConnection(**self._config)
            self._cursorR=self.cn.cursor(prepared=True)
            self._cursorW=self.cn.cursor(prepared=True)

        elif not self.cn.is_connected():
            self.cn.reconnect(2, 0.3)
            self._cursorR=self.cn.cursor(prepared=True)
            self._cursorW=self.cn.cursor(prepared=True)

    def query(self,sql,args=None):
        return self.execute(sql, args)

    def execute(self,sql, args=None, commit=True):
        if re.match(RE_WRITE_ST, sql):
            cursor=self._cursorW
            is_w=True
        else:
            is_w=False
            cursor=self._cursorR
        
        if not args:args=()
        elif not isinstance(args, tuple):
            args=tuple(args)

        try:
                cursor.execute(sql,args)
                if is_w and commit and not self._autocommit:
                    self.cn.commit()
        except:
            if not self.cn.is_connected():
                self.connect()
                cursor.execute(sql,args)
                if is_w and commit and not self._autocommit:
                    self.cn.commit()
            else:
                raise

        if cursor.with_rows:
            return ObjectDict(rows=cursor.fetchall())
        else:
            return cursor.rowcount,cursor.lastrowid

    def commit(self):
        self.cn.commit()
    
    def rollback(self):
        self.cn.rollback()

