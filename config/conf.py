# -*- coding: utf-8 -*-
'''
Created on Jan 18, 2016

@author: sunmoonone
'''
from common.utils import ObjectDict
import os
import yaml
import logging
import logging.config as logging_config

settings=ObjectDict()

settings.http=ObjectDict({
                        'auth_login_id':'uid',
                        'auth_login_path':'/login',
                        'template_path':'templates',
                        'allow_cross_site':False
                      })
settings.profile=ObjectDict()
settings.profile.enabled=False

settings.kmq=ObjectDict()
settings.kmq.encoding='utf-8' 

settings.test=ObjectDict()

class _Logger(logging.Logger):

    def clone(self,name):
        """return a new logger cloned from `self` with a different `name` but with the same handlers
        """
        this = logging.getLogger(name)
        this.propagate=self.propagate
        this.setLevel(self.level)

        if this.handlers:
            return this

        for h in self.handlers:
            this.addHandler(h)
        return this

logging.setLoggerClass(_Logger)
logging.logProcesses=0

_conf_file = os.environ['project_conf'] if 'project_conf' in os.environ else 'conf.yml'

with open(_conf_file) as f:
    conf = yaml.load(f)
    for k,v in conf.items():
        if isinstance(v, dict):
            if k in settings:
                settings[k].update(conf[k])
            else:
                settings[k] = ObjectDict(v)
        else:
            settings[k] = v

    if settings.logging:
        if settings.logging.logfile and not os.path.exists(os.path.dirname(settings.logging.logfile)):
            os.makedirs(os.path.dirname(settings.logging.logfile))

        logging_config.dictConfig(settings.logging)
        
