# dbskit
[![Build Status](https://api.travis-ci.org/listen-lavender/dbskit.svg?branch=master)](https://api.travis-ci.org/listen-lavender/dbskit)

dbskit是一个简单的封装了mongo，mysql的数据库操作的工具集，支持线程和协程；orm的mongo和mysql操作是一致的，都遵循mong语法，例如query(select id, name from user where id = 2)需写成query({'id':2}, {'id':1, 'name':1})，操作的一致就可以轻松切换项目的数据库，只需要修改数据库的配置连接就可以了，当然sqlalchemy这种大型的orm已经支持的很好了，但是dbskit对于小型项目更直观，简单，方便控制.

Maybe you have other choice to cross platform, like sqlalchemy, but dbskit is easy to use and control.

## mysql (mongo syntax operation)

>    - 支持线(协)程池
>    - 基本的orm

## mongo

>    - 支持mongo的bulk操作
>    - 基本的orm

# Getting started

Here is a simple example orm operation for Dbskit:

````python

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


    initDB()

    @withMysql('test', resutype='DICT', autocommit=True)
    def test():
        print transfer({"$or" : [{"id_num" : {"$mod" : [5, 1]}}, {"winner" : True}]}, grand=None, parent='', index=index, condition=condition)
        a = Proxy.queryOne({'ip':'110.52.221.27'}, {'ip':1, 'port':1, 'usenum':1})
        print a
        Proxy.update({'ip':'110.52.221.27'}, {'$inc':{'port':1}, '$set':{'usenum':2}})
        a = Proxy.queryOne({'ip':'110.52.221.27'}, {'ip':1, 'port':1, 'usenum':1})
        print a


    if __name__ == '__main__':
        test()
````

## Installation

To install dbskit, simply:

````bash

    $ pip install dbskit
    ✨🍰✨
````

Satisfaction, guaranteed.

## Documentation

    TODO

## Discussion and support

Report bugs on the *GitHub issue tracker <https://github.com/listen-lavender/dbskit/issues*. 