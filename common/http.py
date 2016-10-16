# -*- coding: utf-8 -*-
'''
Created on May 27, 2015

@author: sunmoonone
'''
import types
import json
from common import errors, utils
import os
import sys
import web
import urllib,httplib
import hashlib
from urllib import urlencode
import logging
import inspect
from common.utils import ObjectDict
import traceback
from datetime import datetime
from json.encoder import JSONEncoder
from common.orm.base import BaseModel
from config import settings
from web.webapi import HTTPError
import urlparse

logger=logging.getLogger('common.http')

# request.headers
# request.path
# request.set_property(get_trace_id, 'zipkin_trace_id', reify=True)
# trace_id = request.zipkin_trace_id
# flags = request.headers.get('X-B3-Flags', '0')
# request.method
# request.path_qs
# request.server_port
  
# response.status_code


class HttpRedirect(HTTPError):
    """A `301 Moved Permanently` redirect."""
    def __init__(self, url, status='301 Moved Permanently', absolute=False, replace_schema_with=None):
        """
        Returns a `status` redirect to the new URL. 
        `url` is joined with the base URL so that things like 
        `redirect("about") will work properly.
        """
        newloc = urlparse.urljoin(web.ctx.path, url)

        if newloc.startswith('/'):
            if absolute:
                home = web.ctx.realhome
            else:
                home = web.ctx.home
            newloc = home + newloc

        if replace_schema_with:
            newloc = '%s%s' % (replace_schema_with, newloc[newloc.find(':'):])

        headers = {
            'Content-Type': 'text/html',
            'Location': newloc
        }
        HTTPError.__init__(self, status, headers, "")

class HttpRequest(ObjectDict):

    def __init__(self,method,action,data,args,params):
        self.action=action
        self.data=data

        self.host=web.ctx.host
        
        self.home = web.ctx.home

        self.path=web.ctx.path
        
        self.fullpath=web.ctx.fullpath
        self.query=web.ctx.query
        self.args=args
        self.params=params
        self.url= web.ctx.homedomain + web.ctx.homepath+web.ctx.fullpath
        
        self._form=None


    def get_header(self,name,failval=None):
# 'HTTP_APU.DOMAIN'
        if(not name in web.ctx.env):
            return failval
        return web.ctx.env[name]

    def form_data(self):
        if self._form:
            return self._form

        if(not self.data):
            return {}
        self._form = utils.parseQuery(self.data)
        
        return self._form
    
    form=property(fget=form_data)

    def raw_data(self):
        return self.data

    def json_data(self):
        if(not self.data):
            return None
        return json.loads(self.data)

    def get_param(self,name):
        if not self.params or name not in self.params:
            raise KeyError('missing param: %s' % name)
        return self.params[name]

def parse_domain():
    host=web.ctx.host
    pos = host.find(':')
    pos1 = host.find('apu.')
    if pos==-1:
        return host[pos1+4:]
    else:
        return host[pos1+4:pos]
 
class RequestHandler(object):
    auth_access_map=()
    _sess=None
    _env=None
    _domain=None

    config = ObjectDict({
                         'auth_login_id':'uid',
                         'auth_login_path':'/login',
                         'template_path':'templates',
                         'url_schema':'http',
                         'allow_cross_site':False
                         })

    config.update(settings.http)


    def _render_template(self,template_name,**context):
        if(not RequestHandler._env):
            from jinja2.environment import Environment
            from jinja2.loaders import FileSystemLoader
            RequestHandler._env = Environment(loader=FileSystemLoader(self.config.template_path))
        return RequestHandler._env.get_template(template_name).render(context)

    @classmethod
    def _set_session(cls,sess):
        cls._sess=sess
    
    def _get_logger(self):
        if not hasattr(self,'_logger'):
            self._logger=logging.getLogger(self.__class__.__name__)
        return self._logger
    logger=property(fget=_get_logger)

    def GET(self,*args,**kw):
        method=self._get_method('_GET')
        params = utils.parseQuery(web.ctx.query)
        params.update(kw)
        return self.__handle(method,'GET',params,None,*args)

    def PUT(self,*args,**kw):
        method=self._get_method('_PUT')
        params = utils.parseQuery(web.ctx.query)
        params.update(kw)
        return self.__handle(method,'PUT',params,web.data(),*args)

    def DELETE(self,*args,**kw):
        method=self._get_method('_DELETE')
        params = utils.parseQuery(web.ctx.query)
        params.update(kw)
        return self.__handle(method,'DELETE',params,web.data(),*args)

    def POST(self,*args,**kw):
        params = utils.parseQuery(web.ctx.query)
        params.update(kw)
        data = web.data()
        if(data):
            try:
                if(data.find('=')!=-1 and data.find('{') ==-1):
                    post = utils.parseQuery(data)
                elif data.find('{') ==0:
                    post = json.loads(data)
                else:
                    post=None
                if post:
                    params.update(post)

            except:
                pass
        method=self._get_method('_POST')
        return self.__handle(method,'POST',params,data,*args)

    def _get_method(self,name):
        if hasattr(self,name):
            boundmethod =getattr(self, name)
            if isinstance(boundmethod, types.MethodType):
                return boundmethod
        return None


    def OPTIONS(self,*args,**kw):
        if self.config.allow_cross_site:
            web.header("Access-Control-Allow-Origin", "*")
            web.header('Access-Control-Allow-Headers', 'Referer, Accept, Origin, User-Agent, Content-Type');
            web.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');

    def _before_handle(self):
        web.header('Content-Type', 'text/html; charset=UTF-8')
        if self.config.allow_cross_site:
            web.header("Access-Control-Allow-Origin", "*")
            web.header('Access-Control-Allow-Headers', 'Referer, Accept, Origin, User-Agent, Content-Type');
            web.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE');

    def __handle(self,boundmethod,method,params,data,*args):
        if not boundmethod:
            action="default_action"
            if(params and 'action' in params):
                action = params['action']
                del params['action']

            if(action.startswith('_') or not hasattr(self, action)):
                return self.error("action:%s is not supported" % action)

            if not hasattr(self,action):
                return self.error("action:%s is not supported" % action)
            boundmethod =getattr(self, action)

            if(not isinstance(boundmethod, types.MethodType)):
                return self.error("action:%s is not supported" % action)

            if(action=="success" or action=='error'):
                return self.error("action:%s is not allowed" % action)

            self._action=action
        else:
            self._action=method

        self._request=HttpRequest(method, self._action, data, args, params)

        try:
            self._before_handle()
        except Exception as e:
            if not errors.DISPLAY_TRACE:
                return json.dumps({"state":errors.code(e), "msg":errors.msg(e)})
            return '\n'.join(traceback.format_exception(*(sys.exc_info())))

        if(self._auth_access()):

            #call by method signature
            spec = inspect.getargspec(boundmethod)
            #ArgSpec(args=['self', 'a', 'b', 'd', 'c'], varargs='args', keywords='kw', defaults=(1, 2))

            topass,kw=[],{}
            c = len(spec.args)-1
            if(c > 0):
                topass=args[:c]

            if(spec.varargs):
                for arg in args[c:]:
                    topass.append(arg)

            if(spec.keywords):
                kw=params
                hc= min(len(topass),c)
                i = 1
                while i <= hc:
                    if(spec.args[i] in kw):
                        del kw[spec.args[i]]
                    i = i + 1

            else:
                lack = c - len(args)
                if(lack>0):
                    lacknames=spec.args[-lack:]
                    for n in lacknames:
                        if(n in params):
                            kw[n]=params[n]

            try:
                return boundmethod(*topass,**kw)
            except Exception as e:

                #skip http redirect 
                if isinstance(e, HttpRedirect):
                    return ''

                logger.exception('handle %s error:' % self._action)

                msg = errors.msg(e)
                if 'takes exactly' in msg and 'argument' in msg:
                    msg='invalid arguments'

                if not errors.DISPLAY_TRACE:
                    return json.dumps({"state":errors.code(e), "msg":msg})
                return '\n'.join(traceback.format_exception(*(sys.exc_info())))
        else:
            return self.error('Forbiden', 403)

    def _redirect(self,url, absolute=False, replace_schema_with=None):
        raise HttpRedirect(url,'303 See Other', absolute=absolute, replace_schema_with=replace_schema_with)

    def _auth_access(self):
        '''assert user login state
        '''
        logined = self._getsession(self.config.auth_login_id)

        count = len(self.auth_access_map)
        if(count == 0):
            if(logined):return True
            self._redirect(self.config.auth_login_path, replace_schema_with=self.config.url_schema)
            return False
        if(count % 2 !=0):
            raise Exception('invalid auth_access_map')
        i=0
        j=1
        while j < count:
            if(self.auth_access_map[i] == "**"):
                if(self.auth_access_map[j] == "anon"):
                    return True
                break
            #endif

            if(self._action == self.auth_access_map[i]):
                if(self.auth_access_map[j] == "anon"):
                    return True
                break
            #endif

            if(self.auth_access_map[i].endswith('**')):
                pre = self.auth_access_map[i].rstrip('*')
                if(self._action.startswith(pre) ):
                    if(self.auth_access_map[j] == 'anon'):
                        return True
                    break
            i=i+1
            j=j+1
        #endwhile
        if(logined):return True
        self._redirect(self.config.auth_login_path, replace_schema_with=self.config.url_schema)
        return False


    @property
    def session(self):
        return self._sess

    @property
    def request(self):
        return self._request

    def _getsession(self,name,default=None):
        try:
            return self._sess.__getattr__(name)
        except AttributeError:
            return default

    def _validate_json(self, data):
        try:
            data = json.loads(data)
            if(data):
                return True
        except:
            return False;
        return False;


    def success(self, data=None,more=None,**kw):
        if(more):
            ret=more
            ret['state']=0
        else:
            ret = {"state":0}

        if(data!=None): ret['data']=data

        if kw: ret.update(kw)

        return json.dumps(ret,cls=JsonEncoder)

    def error(self, msg, state=1):
        if(type(msg) != types.StringType):
            return json.dumps({"state":errors.code(msg), "msg":errors.msg(msg)})
        return json.dumps({"state":state, "msg":msg})

    def _dumps(self,obj):
        return json.dumps(obj)


def webctl(urls, fvars, config, session_store=None,session_init=None):
    usage = "Usage: %s start | stop | restart | test | taillog [lines]" % sys.argv[0]
    _cwd_ = os.path.dirname(os.path.abspath(sys.argv[0]))
    os.chdir(_cwd_)

    port = config.http.server_port
    _pidpath_ = config.http.server_pidfile

    if(len(sys.argv) ==1):
        print usage
        sys.exit(0)
    if(len(sys.argv) >1):
        cmd = sys.argv[1]
        if(cmd=="stop"):
            utils.kill(_pidpath_)
            sys.exit(0)
        elif(cmd=="restart"):
            print 'stopping...'
            utils.kill(_pidpath_)
            print 'starting...'
        elif(cmd !="start"):
            print usage
            sys.exit(0)


    sys.argv[1] = str(port)
    app = web.application(urls, fvars)

    if not os.path.exists(os.path.dirname(_pidpath_)):
        os.makedirs(os.path.dirname(_pidpath_))

    utils.deamonize(_cwd_, _pidpath_, os.path.join(os.path.dirname(_pidpath_),'server.log'))

    if(not session_store):
        session_store = web.session.DiskStore('sessions')
    RequestHandler._set_session(web.session.Session(app,session_store,initializer = session_init))
    app.run()

def http_get(host,port,path,params):
    httpClient = None
    try:
        params = urllib.urlencode(params)
        headers = {"Content-type": "application/x-www-form-urlencoded"
                        , "Accept": "text/plain"}

        httpClient = httplib.HTTPConnection(host, port, timeout=30)
        httpClient.request("GET", path, params, headers)

        response = httpClient.getresponse()
#         print response.status
#         print response.reason
        return response.read()
#         print response.getheaders()
    except Exception as e:
        raise e
    finally:
        if httpClient:
            httpClient.close()

def encrypt_params(params, secret_key):
    keys= params.keys()
    keys.sort()

    s=[]
    for k in keys:
        s.append(k)
        s.append('=')
        s.append(params[k])
    s.append(secret_key)

    params['sig'] = hashlib.md5(''.join(s))

def build_query(params, secret_key = ""):
    if(secret_key):
        encrypt_params(params, secret_key)
    return urlencode(params)

class JsonEncoder(JSONEncoder):
    def default(self,o):
        if isinstance(o, datetime):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(o, BaseModel):
            return o._dict()
        return JSONEncoder.default(self, o)
