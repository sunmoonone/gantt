#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on Dec 29, 2015

@author: sunmoonone
'''
import os
import settings
from config import resource

from common.http import RequestHandler
from common.utils import ObjectDict
from gantt.mediacontroller import MediaController
from gantt.admincontroller import AdminController
from gantt.usercontroller import UserController

urls = (
        '/?$', 'IndexController',
        '/.*\.(js|css|jpeg|png|jpg|gif|html|ttf|eot|svg|woff|woff2|otf)', 'MediaController',
        '/user/?.*', 'UserController',
        '/admin/?.*', 'AdminController',
        )

class IndexController(RequestHandler):
    '''user authentication
    '''

    def default_action(self):
        return self._render_template('index.html')

    def setup(self):
        dbc =resource.dbc(False)
        db='gantt'
        dbc.query('CREATE DATABASE IF NOT EXISTS %s DEFAULT CHARSET utf8 COLLATE utf8_general_ci' % db)
        dbc.use(db)

        User.create_table(dbc, db)
        Task.create_table(dbc, db)
        Project.create_table(dbc, db)
        Link.create_table(dbc, db)

        return self.success()


if __name__ == '__main__':
    pass
#     webctl(urls,globals(),'dev-server.cnf',session_init={'uid':'1','user':ObjectDict({'uid':'1','name':'someone','gname':'dev','gid':0,'domain':'test.com','role':['user']})})
else:
    glbs = globals()
