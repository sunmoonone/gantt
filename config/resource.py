# -*- coding: utf-8 -*-
'''
Created on Dec 18, 2015

@author: sunmoonone
'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')  # @UndefinedVariable

import logging
import os.path
from logging.handlers import RotatingFileHandler
from common.threadlocal import PersistentPool
from common import database
from config.conf import settings

__TEST__=False

ENCODING='utf-8'

class ResourceError(Exception):
    def __init__(self,msg):
        self.message=msg
        self.code=10

    def __str__(self, *args, **kwargs):
        return '%s(%s, %r)' % (self.__class__.__name__, self.code, self.message)

    def __repr__(self, *args, **kwargs):
        return '%s(%s, %r)' % (self.__class__.__name__, self.code, self.message)


_pool=PersistentPool()
resources = settings.resource

#===================
# logging
_default_file=settings['logging']['logfile']
_format=settings['logging']['formatters']['common']['format']

def Logger(name, filename=None, subdir='',maxBytes=20*1024*1024,backupCount=5):
    '''
    NOTE: logging is thread-safe but is not process-safe
    '''
    logger = logging.getLogger(name or settings.logging.default_name)  # @UndefinedVariable

    if not filename:filename=os.path.basename(_default_file)
    logdir=os.path.dirname(_default_file) or '.'

    # file handler
    if subdir:
        logdir=os.path.join(logdir,subdir)
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    filename=os.path.join(logdir,'%s.log' % (filename[0:-4] if filename.endswith('.log') else filename))
    if filename==_default_file:
        return logger
    
    if logger.handlers:
        return logger

    h = RotatingFileHandler(filename,maxBytes=maxBytes, backupCount=backupCount)
    h.setFormatter(logging.Formatter(_format))
    logger.addHandler(h)
    logger.propagate=False

    return logger

#===================

def dbc(withdb=True):
    return _dbc('gantt', 'db', withdb)


def _dbc(domain, group, withdb=True):
    '''get database connection for domain
    '''
    m=resources[domain]
    if not m:
        raise ResourceError('no resource for domain: %s' % domain)
    args = m[group]['master']

    k ='dbc-%s%s' % ( ','.join((str(v) for v in args)),  domain)
    conn = _pool.get_resource(k)
    if(not conn):
        db = domain.replace(".", "_") if withdb else ""

        conn = database.Connection(args[0], int(args[1]), args[2], args[3], db)
        _pool.set_resource(k, conn)
    return conn



