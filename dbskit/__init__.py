#!/usr/bin/python
# coding=utf-8

"""
    
"""

__import__('pkg_resources').declare_namespace(__name__)
__version__ = '1.0.2'
__author__ = 'hk'

def singleton(cls):
    instances = {}
    def _singleton(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return _singleton

class Field(object):
    _count = 0
    def __init__(self, **attributes):
        self.name = attributes.get('name')
        self.ddl = attributes.get('ddl')
        self.pyt = attributes.get('pyt')
        self.default = attributes.get('default')
        self.comment = attributes.get('comment')
        self.nullable = attributes.get('nullable', 1)
        self.unique = attributes.get('unique')
        self.insertable = attributes.get('insertable', True)
        self.deleteable = attributes.get('deleteable', True)
        self.updatable = attributes.get('updatable', True)
        self.queryable = attributes.get('queryable', True)
        Field._count += 1
        self.order = Field._count

    # def __get__(self, obj, cls):
    #     return obj[self.name]
        
    # def __set__(self, obj, value):
    #     obj[self.name] = value

    def check_value(self, value, strict=False):
        if type(value) == self.pyt:
            return value
        if strict:
            raise Exception('Strict mode, field value %s is not right type %s.' % (str(value), str(self.pyt)))
        else:
            if self.default is None:
                raise Exception('Field %s has no default.' % self.name)
            return self.default

    def __str__(self):
        s = ['<%s:%s,%s,default(%s)' % (self.__class__.__name__, self.name or 'None', self.ddl or 'None', self.default or 'None')]
        # self.nullable and s.append('N')
        self.insertable and s.append('I')
        self.deleteable and s.append('D')
        self.updatable and s.append('U')
        self.queryable and s.append('Q')
        s.append('>')
        self.comment and s.append(self.comment or '')
        return ''.join(s)

    @classmethod
    def verify(cls, val):
        pass
