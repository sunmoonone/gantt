# -*- coding: utf-8 -*-
'''
apu settings
'''

import sys,os
import yaml

def choose_conf():
    with open('/opt/gantt_env') as f:
        c = f.read()
        conf_file = 'conf/%s.yml' % c.strip()

    os.environ['project_conf']=conf_file

    with open(conf_file) as f:
        conf = yaml.load(f)
        if conf and 'include_path' in conf:
            for p in reversed(conf['include_path']):
                sys.path.insert(0, p)
choose_conf()

from config import settings