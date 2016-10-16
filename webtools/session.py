# -*- coding: utf-8 -*-
'''
Created on Apr 7, 2016

@author: sunmoonone
'''
from web.session import Store
import time

class CacheInterface(object):

    def exists(self,key):
        raise NotImplementedError
    
    def get(self,key):
        raise NotImplementedError
    
    def set(self,key, pickled, timeout):
        '''
        @param timeout: timeout in seconds 
        '''
        raise NotImplementedError
    
    def delete(self,key):
        '''delete key
        '''
        raise NotImplementedError
    
    def clean(self, now, timeout):
        ''' clean expired keys

        @param now: timestamp returned by time.time() in seconds 
        @param timeout: timeout in seconds 
        '''
        raise NotImplementedError

class RedisCacheInterface(CacheInterface):
    def __init__(self, client):
        '''
        @param client: redis client
        '''
        
        self._r=client

    def exists(self, key):
        return self._r.exists(key)
#         score = self._r.zscore('session_redis_cache_backend', key)
#         if score:
#             return True
#         else:
#             return False
    
    def get(self, key):
        if self.exists(key):
            return self._r.get(key)
    
    def set(self, key, pickled, timeout):
        self._r.set(key, pickled, timeout)
#         self._r.zadd('session_redis_cache_backend', time.time(), key)
        
    def delete(self, key):
        self._r.delete(key)
#         self._r.zrem('session_redis_cache_backend', key)
    
    def clean(self, now, timeout):
        # No need to do cleanup, redis will delete timeout keys automatically
        pass
#         self._r.zremrangebyscore('session_redis_cache_backend', 0, now - timeout)

class CacheStore(Store):
    """
    Store for saving a session to cache

        >>> ci = RedisCacheInterface()
        >>> s = Cache(ci, 20, 'test_session')
        >>> s['a'] = 'foo'
        >>> s['a']
        'foo'
        >>> time.sleep(0.01)
        >>> s.cleanup(0.01)
        >>> s['a']
        Traceback (most recent call last):
            ...
        KeyError: 'a'
    """
    def __init__(self, cache_interface, timeout, key_prefix):
        '''
        @param cache_interface: an object of `CacheInterface` 
        @param timeout: key expires after this timeout
        @param key_prefix:  key prefix, it's useful when multiple web apps use the same cache backend
        '''
        self._ci=cache_interface
        self._timeout=timeout
        self._pre=key_prefix

    def __contains__(self, key):
        key = '%s_%s' % (self._pre, key)
        return self._ci.exists(key)

    def __getitem__(self, key):
        pickled = self._ci.get('%s_%s' % (self._pre, key))
        if pickled:
            return self.decode(pickled)
        else:
            raise KeyError, key

    def __setitem__(self, key, value):
        pickled = self.encode(value)    
        try:
            key = '%s_%s' % (self._pre, key)
            self._ci.set(key,pickled, self._timeout)
        except IOError:
            pass

    def __delitem__(self, key):
        key = '%s_%s' % (self._pre, key)
        self._ci.delete(key)
    
    def cleanup(self, timeout):
        now = time.time()
        self._ci.clean(now, timeout)
