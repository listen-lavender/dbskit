#!/usr/bin/env python
# coding=utf-8

import time
import functools
import threading
import weakref
import traceback
import sys

import handler
from . import CFG
from .. import singleton
from error import ClassAttrNameConflictError, \
                  ConnectionNotFoundError, \
                  ConnectionNameConflictError

MINLIMIT = 10
MAXLIMIT = 40
customattrs = lambda cls:[attr for attr in dir(cls) if not attr.startswith('_')]


class DBConnect(handler.DBHandler):

    def __init__(self, settings, autocommit=True, resutype='DICT'):
        super(DBConnect, self).__init__('', handler.dblib.connect(**settings), resutype=resutype, autocommit=autocommit)

class DBPool(object):

    def __init__(self, markname, minlimit=MINLIMIT, maxlimit=MAXLIMIT, **settings):
        self.markname = markname
        self.settings = settings
        # self.client = handler.dblib.MongoClient(settings['host'], settings['port'], minPoolSize=minlimit, maxPoolSize=maxlimit, waitQueueMultiple=10)
        self.client = handler.dblib.MongoClient(settings['host'], settings['port'], maxPoolSize=maxlimit, waitQueueMultiple=10)

    def __repr__(self):
        return "<%s::%s>" % (self.__class__.__name__, self.markname)

    @property
    def alive(self):
        return 0

    @property
    def peak(self):
        return 0

    def connect(self):
        return self.client

    def release(self, conn):
        pass

@singleton
class DBPoolCollector(object):

    def __init__(self, handler=None, delegate=False):
        self._handler = handler
        self._collection = {}
        self._current = threading.local()
        self._current.connect = None
        self._current.markname = None
        self._current.handler = None
        self.setDelegate(delegate)

    def __getattr__(self, attr):
        if not self._delegate or (attr.startswith('_') or not hasattr(self._current,"handler")):
            return self.__getattribute__(attr)
        else:
            return getattr(self._current.handler, attr)

    def setDelegate(self, delegate):
        if delegate:
            if set(customattrs(self._handler)).intersection(set(customattrs(self))):
                raise ClassAttrNameConflictError("If open delegate, ConnectionHandler's attr name should not appear in DBPoolCollector")
            self._delegate = True
        else:
            self._delegate = False

    def addDB(self, markname, minlimit=MINLIMIT, maxlimit=MAXLIMIT, **settings):
        """
        :param markname: string database name
        :param settings: connection kwargs
        :return:
        """
        override = settings.pop("override", False)
        if not override and self._collection.has_key(markname):
            msg = "Alreay exist connection '%s',override or rename it." % markname
            print msg
            # raise ConnectionNameConflictError(msg)
        else:
            self._collection[markname] = DBPool(markname, minlimit, maxlimit, **settings)

    def deleteDB(self, markname):
        """
        :param markname: string database name
        """
        if self._current.markname == markname:
            self.release()
        if hasattr(self._collection, markname):
            del self._collection[markname]

    def connect(self, markname, connect=None, resutype='DICT', autocommit=False):
        """
        Mapping current connection handler's method to DBPoolCollector
        :return:
        """
        if not hasattr(self._current, "connect") or self._current.connect is None:
            if connect is not None:
                self._current.connect = connect
                self._current.markname = markname
                self._current.handler = handler.DBHandler(markname, self._current.connect, resutype=resutype, autocommit=autocommit, db=self._collection[markname].settings['db'])
            else:
                self._current.connect = self._collection[markname].connect()
                self._current.markname = markname
                self._current.handler = handler.DBHandler(markname, self._current.connect, resutype=resutype, autocommit=autocommit, db=self._collection[markname].settings['db'])

    def release(self):
        """
        :return:
        """
        pass
        # print "start...", self._current.markname, self._current.connect
        # self._collection[self._current.markname].release(self._current.connect)
        # del self._current.handler, self._current.connect
        # print "end..."

    @property
    def handler(self):
        return weakref.proxy(self._current.handler)

    # @staticmethod
    # def instance():
    #     if not hasattr(DBPoolCollector, "_instance"):
    #         with DBPoolCollector._instance_lock:
    #             if not hasattr(DBPoolCollector, "_instance"):
    #                 DBPoolCollector._instance = DBPoolCollector()
    #     return DBPoolCollector._instance

dbpc = DBPoolCollector(handler.DBHandler, delegate=True)

def withMongo(mark, connect=None, resutype='DICT', autocommit=False):
    """
    :param markname:
    :return:the decorator with specific db connection
    """
    def wrapped(fun):
        @functools.wraps(fun)
        def wrapper(*args, **kwargs):
            if hasattr(mark, '__call__'):
                markname = mark()
            else:
                markname = mark
            if not dbpc._collection.has_key(markname):
                raise ConnectionNotFoundError("Not found connection for '%s', use dbpc.addDB add the connection" % markname)
            dbpc.connect(markname, connect=connect, resutype=resutype, autocommit=autocommit)
            try:
                res = fun(*args, **kwargs)
            except:
                raise
            finally:
                dbpc.release()
            return res
        return wrapper
    return wrapped

@withMongo(CFG.R, resutype='DICT')
def withMongoCount(table, spec):
    return dbpc.handler.queryAll(spec, collection=table).count()

@withMongo(CFG.R, resutype='DICT')
def withMongoQuery(table, spec, projection=None, sort=[], skip=0, limit=10, qt='all'):
    if qt.lower() == 'all':
        return dbpc.handler.queryAll(spec, collection=table, projection=projection, sort=sort, skip=skip, limit=limit)
    else:
        return dbpc.handler.queryOne(spec, collection=table, projection=projection, sort=sort, skip=0, limit=1)

@withMongo(CFG.W, autocommit=True)
def withMongoInsert(table, doc, keycol, update=True):
    if update and keycol:
        condition = {}
        for kc in keycol:
            condition[kc] = doc[kc]
        if '_id' in doc:
            del doc['_id']
        return dbpc.handler.update(spec, {'$set':doc}, collection=table, upsert=True)
    else:
        return dbpc.handler.insert(doc, collection=table)

@withMongo(CFG.W, autocommit=True)
def withMongoDelete(table, spec):
    dbpc.handler.delete(spec, collection=table)

@withMongo(CFG.W, autocommit=True)
def withMongoUpdate(table, spec, doc):
    for k in doc:
        if not k.startswith('$'):
            raise Exception("Wrong update doc.")
    dbpc.handler.update(spec, doc, collection=table, upsert=False)

if __name__ == "__main__":

    dbpc.addDB("local", 1, host="127.0.0.1",
                    port=27017,
                    db="dandan-jiang")

    @withMongo('local')
    def test():
        dbpc.handler.insert('hotel_info_collection', {'name':'b', 'pass':'b', 'status':1})
        for key, val in dbpc.handler.showColumns('hotel_info_collection').items():
            print key, val
        for one in dbpc.handler.queryAll('hotel_info_collection', {'name':'a'}):
            print one
        print 'one', dbpc.handler.queryOne('hotel_info_collection', {'name':'b'})
        dbpc.handler.update('hotel_info_collection', {'cond':{'name':'b'}, 'data':{'name':'b', 'pass':'b', 'status':2}})
        dbpc.handler.delete('hotel_info_collection', {'name':'b'})
        print 'one', dbpc.handler.queryOne('hotel_info_collection', {'name':'b'})

    test()