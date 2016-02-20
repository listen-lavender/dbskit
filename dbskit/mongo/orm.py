#!/usr/bin/python
# coding=utf-8
import time, datetime, logging, threading, sys, traceback, hashlib
from suit import dbpc
from bson.objectid import ObjectId
from ..util import rectify
from .. import Field

MAXSIZE = 20


class IdField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = ObjectId()
        attributes['ddl'] = 'ObjectId'
        attributes['pyt'] = ObjectId
        super(IdField, self).__init__(**attributes)

    @classmethod
    def verify(cls, val):
        return ObjectId(val)


class PassField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            m = hashlib.md5()
            origin = '123456'
            m.update(origin)
            secret = m.hexdigest()
            attributes['default'] = m.hexdigest()
        attributes['ddl'] = 'ObjectId'
        attributes['pyt'] = ObjectId
        super(IdField, self).__init__(**attributes)

    @classmethod
    def verify(cls, val):
        m = hashlib.md5()
        origin = '123456'
        m.update(val)
        return m.hexdigest()


class StrField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = ''
        attributes['ddl'] = 'str'
        attributes['pyt'] = str
        super(StrField, self).__init__(**attributes)


class IntField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = 0
        attributes['ddl'] = 'int'
        attributes['pyt'] = int
        super(IntField, self).__init__(**attributes)


class FloatField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = 0.0
        attributes['ddl'] = 'float'
        attributes['pyt'] = float
        super(FloatField, self).__init__(**attributes)


class BoolField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = False
        attributes['ddl'] = 'bool'
        attributes['pyt'] = bool
        super(BoolField, self).__init__(**attributes)


class DatetimeField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = datetime.datetime.now()
        if not 'ddl' in attributes:
            attributes['ddl'] = 'datetime'
        attributes['pyt'] = datetime.datetime
        super(DatetimeField, self).__init__(**attributes)


class ListField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = []
        if not 'ddl' in attributes:
            attributes['ddl'] = 'list'
        attributes['pyt'] = list
        super(ListField, self).__init__(**attributes)


class DictField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = {}
        if not 'ddl' in attributes:
            attributes['ddl'] = 'dict'
        attributes['pyt'] = dict
        super(DictField, self).__init__(**attributes)


_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])

def genDoc(tablename, tablefields):
    pk = None
    uniques = {}
    doc = []
    for f in sorted(tablefields.values(), lambda x, y: cmp(x.order, y.order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % n)
        ddl = f.ddl
        nullable = f.nullable
        if f.unique:
            if f.unique in uniques:
                uniques[f.unique].append(f.name)
            else:
                uniques[f.unique] = [f.name]
        doc.append(nullable and '  "%s":"(%s)"' % (f.name, ddl) or '  "%s":"%s"' % (f.name, str(f.default)))
    return '-- generating DOC for %s: \n %s {\n' % (tablename, tablename) + ',\n'.join(doc) + '};'


class ModelMetaclass(type):
    '''
    Metaclass for model objects.
    '''
    def __new__(cls, name, bases, attrs):
        for b in bases:
            attrs = dict(getattr(b, '__mappings__', {}), **attrs)
        # skip base Model class:
        if name=='Model':
            return type.__new__(cls, name, bases, attrs)

        # store all subclasses info:
        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s' % name)

        logging.info('Scan ORMapping %s...' % name)
        mappings = dict()
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                logging.info('Found mapping: %s => %s' % (k, v))
                mappings[k] = v
        for k in mappings.iterkeys():
            attrs.pop(k)
        attrs['__mappings__'] = mappings
        cls.genDoc = lambda self: genDoc(attrs['__table__'], mappings)
        return type.__new__(cls, name, bases, attrs)


class Model(dict):
    __table__ = None
    __metaclass__ = ModelMetaclass
    _insertdoc = None
    _insertdatas = []
    __lock = None

    def __init__(self, **attributes):
        super(Model, self).__init__(**attributes)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def __setstate__(self, state):
        self.__dict__ = state

    def __getstate__(self):
        return self.__dict__

    @classmethod
    def queryOne(cls, spec, projection=None, sort=[]):
        '''
        Find by where clause and return one result. If multiple results found, 
        only the first one returned. If no result found, return None.
        '''
        rectify(cls, IdField, 'IdField', spec)
        d = dbpc.handler.queryOne(spec, collection=cls.__table__, projection=projection, sort=sort, skip=0, limit=1)
        return d

    @classmethod
    def queryAll(cls, spec, projection=None, sort=[], skip=0, limit=10):
        '''
        Find all and return list.
        '''
        rectify(cls, IdField, 'IdField', spec)
        L = dbpc.handler.queryAll(spec, collection=cls.__table__, projection=projection, sort=sort, skip=skip, limit=limit)
        return L

    @classmethod
    def count(cls, spec):
        '''
        Find by 'select count(pk) from table where ... ' and return int.
        '''
        rectify(cls, IdField, 'IdField', spec)
        return dbpc.handler.queryAll(spec, collection=cls.__table__).count()

    @classmethod
    def insert(cls, obj, update=True, method='SINGLE', forcexe=False, maxsize=MAXSIZE):
        if cls.__lock is None:
            cls.__lock = threading.Lock()
        record = None
        if obj is not None and update:
            updatekeys = {}
            for k, v in obj.__mappings__.iteritems():
                if v.unique:
                    updatekeys[k] = obj[k]
            if updatekeys:
                record = dbpc.handler.queryOne(updatekeys, collection=cls.__table__)
            if record and 'create_time' in record:
                obj['create_time'] = record['create_time']
        if record:
            dbpc.handler.update(updatekeys, {"$set":obj}, collection=cls.__table__)
        else:
            if method == 'SINGLE':
                try:
                    if obj:
                        return dbpc.handler.insert(obj, collection=cls.__table__, method=method) #, bypass_document_validation=update)
                except:
                    t, v, b = sys.exc_info()
                    err_messages = traceback.format_exception(t, v, b)
                    print(': ', ','.join(err_messages), '\n')
            else:
                with cls.__lock:
                    if obj is not None:
                        cls._insertdatas.append(obj)
                    if forcexe:
                        try:
                            if cls._insertdatas:
                                dbpc.handler.insert(cls._insertdatas, collection=cls.__table__, method=method) #, bypass_document_validation=update)
                                cls._insertdatas = []
                        except:
                            t, v, b = sys.exc_info()
                            err_messages = traceback.format_exception(t, v, b)
                            print(': ', ','.join(err_messages), '\n')
                    else:
                        if sys.getsizeof(cls._insertdatas) > maxsize:
                            try:
                                dbpc.handler.insert(cls._insertdatas, collection=cls.__table__, method=method) #, bypass_document_validation=update)
                                cls._insertdatas = []
                            except:
                                t, v, b = sys.exc_info()
                                err_messages = traceback.format_exception(t, v, b)
                                print(': ', ','.join(err_messages), '\n')

    @classmethod
    def delete(cls, spec):
        rectify(cls, IdField, 'IdField', spec)
        dbpc.handler.delete(spec, collection=cls.__table__)

    @classmethod
    def update(cls, spec, doc):
        for k in doc:
            if not k.startswith('$'):
                raise Exception("Wrong update doc.")
        rectify(cls, IdField, 'IdField', spec)
        dbpc.handler.update(spec, doc, collection=cls.__table__)


if __name__=='__main__':
    pass
