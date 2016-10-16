'''
Created on Apr 28, 2015

@author: sunmoonone
'''
import  cPickle
import urlparse
from datetime import datetime
import time
import sys
import os.path
import os.path as ospath
import signal
import inspect
import random
import hashlib

import socket
import struct
import re
from urllib import urlencode
try:
    import fcntl  # @UnresolvedImport
except:
    pass


def get_interface_ip(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', ifname[:15])
    )[20:24])

def get_lan_ip():
    ip = socket.gethostbyname(socket.gethostname())
    if ip.startswith("127."):
        for ifname in ( "eth0", "eth1", "eth2", "wlan0", "wlan1", "wifi0", "ath0", "ath1", "ppp0"):
            try:
                ip = get_interface_ip(ifname)
                break
            except IOError:
                pass
    return ip

def randint(ndigits=4):
    return int(round(random.random(),ndigits)*pow(10,ndigits))

def gen_id(number,rand_len=3):
    t = ( long(time_since(2015,True)), number, randint(rand_len))
    return int_to_36(long("%s%s%s" % t))

def unique_id(unique_prefix,rand_len=3):
    return hashlib.md5('%s%s%s' % (unique_prefix, time_since(2015,True), 
                                   randint(rand_len))).hexdigest()

def int_to_36(n):
    '''
    @return str
    '''
    loop = '0123456789abcdefghijklmnopqrstuvwxyz'
    a = []
    while n != 0:
        a.append( loop[n % 36] )
        n = n / 36
    a.reverse()
    return ''.join(a)

def serialize(obj):
    return cPickle.dumps(obj, cPickle.HIGHEST_PROTOCOL)

def deserialize(content):
    return cPickle.loads(content)

def parseQuery(query, from_encoding='iso-8859-1'):
    if(not query):return {}

    if from_encoding:
        query =query.encode(from_encoding)

    params = urlparse.parse_qs(query.lstrip('?'), True)
    params = ObjectDict(params)
    flat_params(params, False)
    return params

def querify(values):
    return urlencode(values)

def flat_params(params, strong_flat=True):
    """convert values of type list to comma separated str
    @param params:the parsed params
    @param strong_flat: k=[v] will be converted to k=v and k=[v1,v2] converted to k='v1,v2'
    if strong_flat=false: k=[v] will be parsed to k=v and k=[v1,v2] will not be converted
    """
    for k,v in params.items():
        if isinstance(v,list):
            if(strong_flat):
                params[k] = (','.join(v)).decode('utf8')

            elif len(v)==1:
                params[k] = v[0].decode('utf8')

        elif v:
            params[k]=v.decode('utf8')

class ObjectDict(dict):
    def __getattr__(self, name):
        if(name.startswith('__')):
            return dict.__getattribute__(self,name)
        try:
            return self.__getitem__(name)
        except KeyError:
            return None

    def __setattr__(self, name, value):
        self.__setitem__(name, value)

    def __delattr__(self, name):
        self.__delitem__(name)

class Number(object):
    def __init__(self, val):
        self._val=val
    
    def pre_inc(self,delta):
        '''
        ++n
        '''
        val=self._val
        self._val+=delta
        return val

    def post_inc(self,delta):
        '''
        n++
        '''
        self._val+=delta
        return self._val

class TimeCounter(list):
    '''
    
    >>>counter = TimeCounter('1h')
    >>>counter.inc(1) 
    >>>time.sleep(1)
    >>>counter.inc(1) 
    >>>time.sleep(1)
    >>>counter.inc(1) 
    >>>counter.inc(1) 
    >>>counter
    [ ['1',1],['2',1], ['3',2] ]

    '''
    reg = '^(\d+)([mdhMs])$'

    def __init__(self, iterable, tick='1h', tick_count=24):
        '''
        @param tick: format `\d+[mdhMs]` where m d h M s corresponds to month day hour minute and second
        eg: 3h for every 3 hours, 30m for every 30 minutes, 10s for every 10 seconds
        '''
        super(TimeCounter,self).__init__(iterable)
        m=re.match(self.reg, tick)
        if not m:
            raise ValueError('invalid format for tick')

        self._num = int(m.group(1))
        self._tick=m.group(2)
        self._maxlen=max(1,tick_count)
        
        if len(self):
            self._last=self[-1]
            #TODO:verify data format

        else:
            self._last=None
    
    def update(self, delta):
        if self._tick=='m':
            f='%y-%m'

            if self._last:
                t = time.mktime(time.strptime(self._last[0], f))
                if (time.time() -t) < self._num * 30 * 24 * 3600:
                    self._last[1] += delta
                    return

            self.append([time.strftime(f),delta])
            self._last=self[-1]
            
        elif self._tick=='d':
            f='%y-%m-%d'

            if self._last:
                t = time.mktime(time.strptime(self._last[0], f))
                if (time.time() -t) < self._num * 24* 3600:
                    self._last[1] += delta
                    return

            self.append([time.strftime(f),delta])
            self._last=self[-1]
            
        elif self._tick=='h':
            f='%y-%m-%d %H'

            if self._last:
                t = time.mktime(time.strptime(self._last[0], f))
                if (time.time() -t) < self._num * 3600:
                    self._last[1] += delta
                    return

            self.append([time.strftime(f),delta])
            self._last=self[-1]
            
        elif self._tick=='M':
            f='%y-%m-%d %H:%M'

            if self._last:
                t = time.mktime(time.strptime(self._last[0], f))
                if (time.time() -t) < self._num * 60:
                    self._last[1] += delta
                    return

            self.append([time.strftime(f),delta])
            self._last=self[-1]

        elif self._tick=='s':
            f='%y-%m-%d %H:%M:%S'

            if self._last:
                t = time.mktime(time.strptime(self._last[0], f))
                if (time.time() -t) < self._num:
                    self._last[1] += delta
                    return

            self.append([time.strftime(f),delta])
            self._last=self[-1]
        
        if len(self) > self._maxlen:
            self.pop(0)

class List(list):
    '''List that can limit the count of elements
    '''
    
    def __init__(self, iterable, maxlen=None):
        super(List, self).__init__(iterable)
        self._maxlen=maxlen
        if self._maxlen and len(self) > self._maxlen:
            count = len(self) - self._maxlen
            del self[0: count]
    
    def extend(self, iterable):
        super(List, self).extend(iterable)
        if self._maxlen and len(self) > self._maxlen:
            count = len(self) - self._maxlen
            del self[0: count]
    
    def append(self, item):
        super(List, self).append(item)
        if self._maxlen and len(self) > self._maxlen:
            del self[0]
    

def file_get_contents(path):
    if not os.path.exists(path):
        return None

    f = open(path)
    try:
        return f.read()
    finally:
        if f:
            f.close()

def file_put_contents(path,content):
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path))

    f = open(path,'w')
    try:
        f.write(content)
    finally:
        if f:
            f.close()

def makedirs(filename):
    if not os.path.exists(filename):
        p=os.path.dirname(filename)
        if p and not os.path.exists(p):
            os.makedirs(p)

def timestamp(dt=None):
    if(dt):
        return int((dt - datetime(1970, 1, 1)).total_seconds())
    return int(time.time())

def now():
    return datetime.now()

def time_since_2010(milli=False):
    '''Return the current time in seconds(or in milliseconds ) since the 2010
    '''
    t = (datetime.now() - datetime(2010, 1, 1)).total_seconds()
    if milli:
        return round(t * 1000)
    return round(t)

def time_since(year,milli=False):
    '''
    @return int: the current time in seconds(or in milliseconds ) since the 2010
    '''
    t = (datetime.now() - datetime(year, 1, 1)).total_seconds()
    if milli:
        return int(round(t * 1000))
    return int(round(t))

def strftime(dt=None,fmt='%Y-%m-%d %H:%M:%S'):
    if(not dt):dt=datetime.now()
    return dt.strftime(fmt)


def deamonize(wd, pidfile, logfile, onsigterm=None, onsighup=None, rediret_to_fd=None):
    """
    Fork the process into the background.
    """

    if(os.path.exists(pidfile)):
        pf = file(pidfile, 'r')
        content = pf.read().strip()
        if(content):
            sys.stderr.write("please stop it first, found pid: %s\n" % content)
            sys.exit(1)
    
    try:
        lockfile = open(pidfile, "w")
    except IOError:
        sys.stderr.write("Unable to create the pidfile.")
        sys.exit(1)

    try:
        # Try to get an exclusive lock on the file. This will fail if another process has the file
        # locked.
        fcntl.flock(lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        sys.stderr.write("Unable to lock on the pidfile.")
        sys.exit(1)

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        sys.stderr.write("fork #1 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

    os.chdir(wd)
    os.umask(0)
    os.setsid()

    try:
        pid = os.fork()
        if pid > 0:
            sys.exit(0)
    except OSError as e:
        sys.stderr.write("fork #2 failed: (%d) %s\n" % (e.errno, e.strerror))
        sys.exit(1)

#     si = file("stdin.txt", 'r')
    pid = str(os.getpid())

    sys.stdout.write("\nserver started with pid %s\n" % pid)
    sys.stdout.flush()

    file(pidfile, 'w+').write("%s\n" % pid)

    devnull = "/dev/null"
    if hasattr(os, "devnull"):
        devnull = os.devnull
    devnull_fd = os.open(devnull, os.O_RDWR)
    os.dup2(devnull_fd, 0)

    if not rediret_to_fd:
        rediret_to_fd=devnull_fd
    os.dup2(rediret_to_fd, sys.stderr.fileno())
    os.dup2(rediret_to_fd, sys.stdout.fileno())

    if(onsigterm):
        signal.signal(signal.SIGTERM, onsigterm)
    if(onsighup):
        signal.signal(signal.SIGHUP, onsighup)

def send_sig(pid, sig):
    os.kill(pid, sig)

def read_pid(pidfile):
    try:
        pid = None
        if(not os.path.exists(pidfile)):
            return pid
        pf = file(pidfile, 'r')
        content = pf.read().strip()
        if content:
            pid = int(content)
        pf.close()
        return pid
    except IOError:
        return None

def kill(pidfile):
    '''
    called inside a daemon process
    '''
    pid = None

    try:
        if(not os.path.exists(pidfile)):
            print "no such file: %s" % pidfile
            sys.exit(1)
        
    except IOError as e:
        print "IOError %s" % e

    try:
        
        lockfile = file(pidfile, 'r')
        content = lockfile.read().strip()
        if content:
            pid = int(content)

        fcntl.flock(lockfile, fcntl.LOCK_UN)
        lockfile.close()
        os.remove(pidfile)

    except IOError as e:
        sys.stderr.write("Unable to read pid %s" % e)
        sys.exit(1)

    try:
        while 1:
            os.kill(pid, signal.SIGKILL)
            time.sleep(1)
    except OSError as err:
        err = str(err)
        if err.find("No such process") > 0:
            try:
                os.remove(pidfile)
            except OSError:
                pass
        else:
            print str(err)
            sys.exit(1)

def call_by_argspec(boundmethod, args, params):
    """invoke a boundmethod with arguments passed by argspec
    """
    spec = inspect.getargspec(boundmethod)
    # ArgSpec(args=['self', 'a', 'b', 'd', 'c'], varargs='args', keywords='kw', defaults=(1, 2))

    topass, kw = [], {}
    c = len(spec.args) - 1
    if(c > 0):
        topass = args[:c]

    if(spec.varargs):
        for arg in args[c:]:
            topass.append(arg)

    if(spec.keywords):
        kw = params
        hc = min(len(topass), c)
        i = 1
        while i <= hc:
            if(spec.args[i] in kw):
                del kw[spec.args[i]]
            i = i + 1

    else:
        lack = c - len(args)
        if(lack > 0):
            lacknames = spec.args[-lack:]
            for n in lacknames:
                if(n in params):
                    kw[n] = params[n]

    return boundmethod(*topass, **kw)

def walk(path,visit,arg):
    '''Calls the function visit with arguments (arg, dirname, item) for each item in the directory tree rooted at path (including path itself, if it is a directory).
    The argument dirname specifies the visited directory,
    the argument item is the current item in the directory (gotten from os.listdir(dirname)).

    if function visit returns false then `item` will not be visited
    if function visit returns true then walk is aborted
    '''
    for f in os.listdir(path):
        ret = visit(arg,path,f)
        if ret==False:
            continue
        elif ret==True:
            return 0
        if ospath.isdir(ospath.join(path,f)):
            if walk(ospath.join(path,f), visit, arg)==0:
                return 0


def tail(f, count=10, buf_size = 1024):
    """
    @param f: a file object or a file path
    Returns the last `count` lines of file `f` as a list.
    """
    if count == 0:
        return []

    do_close=False
    if isinstance(f,basestring):
        f=open(f,'r')
        do_close=True


    f.seek(0, 2)
    bytes = f.tell()
    line_count = count + 1
    block = -1
    data = []

    while line_count > 0 and bytes > 0:
        if bytes - buf_size > 0:
            # Seek back one whole BUFSIZ
            f.seek(block * buf_size, 2)
            # read BUFFER
            data.insert(0, f.read(buf_size))
        else:
            # file too small, start from begining
            f.seek(0,0)
            # only read what was not read
            data.insert(0, f.read(bytes))
        line_count -= data[0].count('\n')
        bytes -= buf_size
        block -= 1

    if do_close:
        f.close()
    return ''.join(data).splitlines()[-count:]

def join(separator, iterable):
    return separator.join((str(v) for v in iterable))

def deep_update(to, from_):
    """
    Recursively update a dict.
    Subdict's won't be overwritten but also updated.
    """
    for key, value in from_.iteritems(): 
        if key not in to:
            to[key] = value
        elif isinstance(value, dict):
            if isinstance(to[key], dict):
                deep_update(to[key], value) 
            else:
                raise ValueError('cannot merge dict to scalar: %s' % key)

class JsonHook(object):    
    def decode(self,dct):
        '''return a python object from dict `dct`
        '''
        raise NotImplemented('not implemented')
    
    def encode(self,obj):
        '''return a serializable version of obj or raise TypeError
        '''
        raise NotImplemented('not implemented')
