#!/usr/bin/python
# coding=utf-8
from datakit.mongo.orm import *
from datakit.mongo.suit import dbpc, withMongo

class MarkModel(Model):

    def __init__(self, **attributes):
        self.__mappings__['create_time'] = DatetimeField(ddl='datetime')
        self.__mappings__['update_time'] = DatetimeField(ddl='datetime')
        self.__mappings__['tid'] = IntField(ddl='int')
        attributes['create_time'] = attributes.get('create_time', datetime.datetime.now())
        attributes['update_time'] = attributes.get('update_time', datetime.datetime.now())
        for key in self.__mappings__:
            if not key in attributes:
                raise Exception('Need field %s. ' % key)
            attributes[key] = self.__mappings__[key].check_value(attributes[key])
        super(Model, self).__init__(**attributes)

    def __setstate__(self, state):
        self.__dict__ = state

    def __getstate__(self):
        return self.__dict__


def initDB():
    dbpc.addDB('test', 20, host='localhost',
                port=27017,
                user='root',
                passwd='',
                db='dandan-jiang')
    dbpc.addDB('test', 20, host='localhost',
                port=27017,
                db='dandan-jiang')

'''
@comment('代理数据')
'''
class Proxy(MarkModel):
    __table__ = 'proxy'
    ip = StrField(ddl='str', unique='daili')
    port = IntField(ddl='int', unique='daili')
    location = StrField(ddl='str')
    safetype = StrField(ddl='str')
    protocol = StrField(ddl='str')
    refspeed = FloatField(ddl='float')
    usespeed = FloatField(ddl='float')
    usenum = IntField(ddl='int')
    status = IntField(ddl='int')


class ProxyLog(Model):
    __table__ = 'grab_proxy_log'
    pid = IntField(ddl='int(11)')
    elapse = FloatField(ddl='float')
    create_time = DatetimeField(ddl='datetime')

initDB()

@withMongo('test', resutype='DICT')
def test():
    # a = Proxy.queryAll('where ip in (%s, %s)', ('110.52.221.27', '110.72.26.214'))
    for one in Proxy.queryAll({}, {'ip':1, 'port':1}, skip=7, limit=10):
        print one
    print '-------'
    a = Proxy.queryOne({'ip':'110.52.221.27'}, {'ip':1, 'port':1, 'usenum':1})
    print a
    Proxy.update({'ip':'110.52.221.27'}, {'$inc':{'port':1}, '$set':{'usenum':2}})
    a = Proxy.queryOne({'ip':'110.52.221.27'}, {'ip':1, 'port':1, 'usenum':1})
    print a
    


if __name__ == '__main__':
    test()
    