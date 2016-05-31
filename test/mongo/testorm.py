#!/usr/bin/env python
# coding=utf-8
from dbskit.mongo.orm import *
from dbskit.mongo.suit import dbpc, withMongo

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
                port=27018,
                user='root',
                passwd='',
                db='dandan-jiang')
    dbpc.addDB('test', 20, host='localhost',
                port=27018,
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
    protocol = StrField(ddl='str')
    create_time = DatetimeField(ddl='datetime')

initDB()

@withMongo('test', resutype='DICT')
def test():
    # a = Proxy.queryAll('where ip in (%s, %s)', ('110.52.221.27', '110.72.26.214'))
    pl = ProxyLog(pid=1, elapse=1, protocol='我们', create_time='2013-01-01 10:10:10')
    print str(pl)
    


if __name__ == '__main__':
    test()
    