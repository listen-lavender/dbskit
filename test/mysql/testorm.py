#!/usr/bin/env python
# coding=utf-8
from dbskit.mysql import transfer
from dbskit.mysql.orm import *
from dbskit.mysql.suit import dbpc, withMysql

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
                port=3306,
                user='root',
                passwd='',
                db='pholcus',
                charset='utf8',
                use_unicode=False,
                override=False)
    dbpc.addDB('test', 20, host='localhost',
                port=3306,
                user='root',
                passwd='',
                db='pholcus',
                charset='utf8',
                use_unicode=False,
                override=False)

'''
@comment('代理数据')
'''
class Proxy(MarkModel):
    __table__ = 'grab_proxy'
    ip = StrField(ddl='str', unique='daili')
    port = IntField(ddl='int(5)', unique='daili')
    location = StrField(ddl='varchar(30)')
    safetype = StrField(ddl='varchar(30)')
    protocol = StrField(ddl='varchar(30)')
    refspeed = FloatField(ddl='float')
    usespeed = FloatField(ddl='float')
    usenum = IntField(ddl='int(10)')
    status = IntField(ddl='tinyint(1)')


class ProxyLog(Model):
    __table__ = 'grab_proxy_log'
    pid = IntField(ddl='int(11)')
    elapse = FloatField(ddl='float')
    create_time = DatetimeField(ddl='datetime')


def test_transfer():
    print '=============1'
    index = []
    condition = []
    print transfer({"age" : 27, "ab": "c"}, grand=None, parent='', index=index, condition=condition)
    print condition
    print index
    print '=============2'
    index = []
    condition = []
    print transfer({'$or':[{"age" : 27, "ab": "c"}, {'ab': {'$ne':'b'}}]}, grand=None, parent='', index=index, condition=condition)
    print condition
    print index
    print '=============3'
    index = []
    condition = []
    print transfer({"$or" : [{"ticket_no" : 725}, {"winner" : True}]}, grand=None, parent='', index=index, condition=condition)
    print condition
    print index
    print '=============4'
    index = []
    condition = []
    print transfer({"age" : {"$gte" : 18, "$lte" : 30}}, grand=None, parent='', index=index, condition=condition)
    print condition
    print index
    print '=============5'
    index = []
    condition = []
    print transfer({"ticket_no" : {"$in" : [725, 542, 390]}}, grand=None, parent='', index=index, condition=condition)
    print condition
    print index
    print '=============6'
    index = []
    condition = []
    print transfer({"$or" : [{"ticket_no" : {"$in" : [725, 542, 390]}}, {"winner" : True}]}, grand=None, parent='', index=index, condition=condition)
    print condition
    print index
    print '=============7'
    index = []
    condition = []
    print transfer({"$or" : [{"id_num" : {"$mod" : [5, 1]}}, {"winner" : True}]}, grand=None, parent='', index=index, condition=condition)
    print condition
    print index
    print '============='


initDB()

@withMysql('test', resutype='DICT', autocommit=True)
def test():
    import pymongo
    mc = pymongo.MongoClient('localhost')
    ddj = mc['dandan-jiang']
    # a = Proxy.queryAll('where ip in (%s, %s)', ('110.52.221.27', '110.72.26.214'))
    for one in Proxy.queryAll({}, skip=7, limit=10):
        print one
    a = Proxy.queryOne({'ip':'110.52.221.27'}, {'ip':1, 'port':1, 'usenum':1})
    print a
    Proxy.update({'ip':'110.52.221.27'}, {'$inc':{'port':1}, '$set':{'usenum':2}})
    a = Proxy.queryOne({'ip':'110.52.221.27'}, {'ip':1, 'port':1, 'usenum':1})
    print a


if __name__ == '__main__':
    test()
    