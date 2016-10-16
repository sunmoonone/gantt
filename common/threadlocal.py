"""
Implements steady, thread-affine persistent connection to backend server

Created on Jul 16, 2015

@modifier: sunmoonone

This module provides a Python version of the threading.local class.
It is avaliable as _threading_local in the standard library since Python 2.4.

Depending on the version of Python you're using, there may be a faster
threading.local class available in the standard library.

However, the C implementation turned out to be unusable with mod_wsgi,
since it does not keep the thread-local data between requests.
To have a reliable solution that works the same with all Python versions,
we fall back to this Python implemention in DBUtils.

"""

__all__ = ["local"]


try:
    from threading import current_thread
except ImportError: # Python >2.5
    from threading import currentThread as current_thread
from threading import RLock, enumerate


class PersistentResource(object):
    '''

    '''
    def __init__(self, creator, threadlocal=None,*args,**kwargs):
        """Set up the persistent resource generator.

        creator: either an arbitrary function returning new client or a class to construct a new client

        threadlocal: an optional class for representing thread-local data
            that will be used instead of our Python implementation
            (threading.local is faster, but cannot be used in all cases)

        args, kwargs: the parameters that shall be passed to the creator
            function or the constructor of class
        """
        self._creator = creator
        self._args, self._kwargs = args, kwargs
        self.thread = (threadlocal or ThreadingLocal)()
        
    def resource(self):
        try:
            return self.thread.res
        except (AttributeError,KeyError):
            self.thread.res = self._creator(*self._args,**self._kwargs)
            return self.thread.res
        


            
class PersistentPool(object):
    '''
    '''
    def __init__(self):
        self._thread = ThreadingLocal()

    def get_resource(self, key):
        try:
            self._thread.pool
        except (AttributeError,KeyError):
            self._thread.pool={}

        if(not self._thread.pool.has_key(key)):
            return None
        return self._thread.pool[key]

    def set_resource(self, key, res):
        try:
            self._thread.pool
        except (AttributeError,KeyError):
            self._thread.pool={}

        self._thread.pool[key]=res
    
        
class _localbase(object):
    __slots__ = '_local__key', '_local__args', '_local__lock'

    def __new__(cls, *args, **kw):
        self = object.__new__(cls)
        key = '_local__key', 'thread.local.' + str(id(self))
        object.__setattr__(self, '_local__key', key)
        object.__setattr__(self, '_local__args', (args, kw))
        object.__setattr__(self, '_local__lock', RLock())
        object.__setattr__(self, '_vals_', {})

        if args or kw and (cls.__init__ is object.__init__):
            raise TypeError("Initialization arguments are not supported")

        d = object.__getattribute__(self, '__dict__')
        current_thread().__dict__[key] = d 
        return self


def _patch(self):
    '''
    switch __dict__ between threads
    '''
    key = object.__getattribute__(self, '_local__key')
    d = current_thread().__dict__.get(key)
    if d is None:
        d = {'_vals_':{}}
        current_thread().__dict__[key] = d
        object.__setattr__(self, '__dict__', d)
        cls = type(self)
        if cls.__init__ is not object.__init__:
            args, kw = object.__getattribute__(self, '_local__args')
            cls.__init__(self, *args, **kw)
    else:
        object.__setattr__(self, '__dict__', d)


class ThreadingLocal(_localbase):
    """A class that represents thread-local data."""

    def __getattribute__(self, name):
        lock = object.__getattribute__(self, '_local__lock')
        lock.acquire()
        try:
            _patch(self)
            if isinstance(name,basestring) and name.startswith('__') and name.endswith('__'):
                return object.__getattribute__(self, name)

            d = object.__getattribute__(self,'_vals_')
            return d[name] #object.__getattribute__(self, name)
        finally:
            lock.release()

    def __setattr__(self, name, value):
        lock = object.__getattribute__(self, '_local__lock')
        lock.acquire()
        try:
            _patch(self)
            d = object.__getattribute__(self,'_vals_')
            return dict.__setitem__(d,name, value)#object.__setattr__(self, name, value)
        finally:
            lock.release()

    def __delattr__(self, name):
        lock = object.__getattribute__(self, '_local__lock')
        lock.acquire()
        try:
            _patch(self)
            d = object.__getattribute__(self,'_vals_')
            return dict.__delitem__(d,name)#object.__delattr__(self, name)
        finally:
            lock.release()
    
    def __setitem__(self,name,value):
        return self.__setattr__(name, value)
    def __getitem__(self,name):
        return self.__getattribute__(name)

    def __contains__(self,name):
        lock = object.__getattribute__(self, '_local__lock')
        lock.acquire()
        try:
            _patch(self)

            d = object.__getattribute__(self,'_vals_')
            return name in d
        finally:
            lock.release()

    def __delitem__(self,name):
        return self.__delattr__(name)
    
    def __clear__(self):
        lock = object.__getattribute__(self, '_local__lock')
        lock.acquire()
        try:
            _patch(self)

            d = object.__getattribute__(self,'_vals_')
            d.clear()
        finally:
            lock.release()

    def __del__(self):
        try:
            key = object.__getattribute__(self, '_local__key')
            threads = list(enumerate())
        except:
            return
        for thread in threads:
            try:
                _dict = thread.__dict__
            except AttributeError:
                continue
            if key in _dict:
                try:
                    del _dict[key]
                except KeyError:
                    pass
