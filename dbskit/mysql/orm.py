#!/usr/bin/env python
# coding=utf-8
import time, datetime, logging, threading, sys, traceback, hashlib
from suit import dbpc
from . import CFG
from ..util import rectify, transfer
from .. import Field

ORDER = {1:'asc', -1:'desc'}


class IdField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = 0
        attributes['ddl'] = '%s(%d) not null auto_increment' % ('int', 11)
        attributes['pyt'] = int
        super(IdField, self).__init__(**attributes)

    @classmethod
    def verify(cls, val):
        return int(val)


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
        attributes['ddl'] = '%s(%d)' % (attributes.get('ddl', 'varchar'), attributes.get('max_length', 255))
        attributes['pyt'] = str
        super(StrField, self).__init__(**attributes)


class IntField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = 0
        attributes['ddl'] = '%s(%d)' % (attributes.get('ddl', 'int'), attributes.get('max_length', 11))
        attributes['pyt'] = int
        super(IntField, self).__init__(**attributes)


class FloatField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = 0.0
        if not 'ddl' in attributes or attributes['ddl'] == 'float':
            attributes['ddl'] = 'float'
        else:
            attributes['ddl'] = '%s(%d)' % (attributes.get('ddl', 'double'), attributes.get('max_length', 11))
        attributes['pyt'] = float
        super(FloatField, self).__init__(**attributes)


class BoolField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = False
        if not 'ddl' in attributes:
            attributes['ddl'] = 'bool'
        attributes['pyt'] = bool
        super(BoolField, self).__init__(**attributes)


class TextField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = ''
        if not 'ddl' in attributes:
            attributes['ddl'] = 'text'
        attributes['pyt'] = str
        super(TextField, self).__init__(**attributes)


class DatetimeField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = datetime.datetime.now()
        if not 'ddl' in attributes:
            attributes['ddl'] = 'datetime'
        if attributes['ddl'] == 'timestamp':
            self.default = 'current_timestamp on update current_timestamp'
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

def genDoc(cls):
    tablename = cls.__table__
    tablefields = cls.__mappings__
    pk = None
    uniques = {}
    doc = ['-- generating DOC for %s:' % tablename, 'create table if not exists `%s` (' % tablename]

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
        doc.append(nullable and '  `%s` %s,' % (f.name, ddl) or '  `%s` %s not null default %s,' % (f.name, ddl, f.default))

    if uniques:
        doc.append('  primary key (`%s`),' % 'id' if cls.id_name == '_id' else cls.id_name)
        doc.append(',\n'.join('  unique key `%s` (%s)' % (key, ','.join('`'+one+'`' for one in val)) for key, val in uniques.items()))
    else:
        doc.append('  primary key (`%s`)' % 'id' if cls.id_name == '_id' else cls.id_name)
    doc.append(');')
    return '\n'.join(doc)


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
            pass

        mappings = dict()
        search = {}
        has_id = False
        cls.id_name = '_id'
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                mappings[k] = v
                if v.searchable:
                    search[k] = v.searchable
                if v.primary:
                    has_id = True
                    cls.id_name = v.name

        if not has_id:
            attrs[cls.id_name] = IdField(primary=True)
            attrs[cls.id_name].name = cls.id_name
            mappings[cls.id_name] = attrs[cls.id_name]

        for k in mappings.iterkeys():
            attrs.pop(k)
        attrs['__mappings__'] = mappings
        attrs['__search__'] = search
        cls.genDoc = lambda cls:genDoc(cls)
        return type.__new__(cls, name, bases, attrs)


class Model(dict):
    __table__ = None
    __metaclass__ = ModelMetaclass
    expire = None
    _insertsql = None
    _insertdatas = []
    _insertstamp = time.time()
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
    def queryOne(cls, spec, projection={}, sort=[], update=False):
        '''
        Find by where clause and return one result. If multiple results found, 
        only the first one returned. If no result found, return None.
        '''
        keys = []
        args = []
        rectify(cls, '_id', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        if projection:
            projection['_id'] = projection.get('_id', 1)
            projection = ['id' if k == '_id' else k for k, v in projection.items() if v == 1]
        else:
            projection = ['id' if k == '_id' else k for k in cls.__mappings__.keys()]
        projection = ','.join(['`%s` as _id' % c if c == 'id' else '`%s`' % c for c in projection])
        if sort:
            sort = 'order by ' + ','.join(['%s %s' % (one[0], ORDER.get(one[-1], 'asc')) for one in sort])
        else:
            sort = ''
        if update:
            update = 'for update'
        else:
            update = ''
        if where:
            where = 'where %s' % where
        d = dbpc.handler.queryOne('select %s from `%s` %s %s limit %d, %d %s' % (projection, cls.__table__, where, sort, 0, 1, update), [args[index][one] for index, one in enumerate(keys)])
        return d

    @classmethod
    def queryAll(cls, spec, projection={}, sort=[], skip=0, limit=None, update=False):
        '''
        Find all and return list.
        '''
        keys = []
        args = []
        rectify(cls, '_id', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        if projection:
            projection['_id'] = projection.get('_id', 1)
            projection = ['id' if k == '_id' else k for k, v in projection.items() if v == 1]
        else:
            projection = ['id' if k == '_id' else k for k in cls.__mappings__.keys()]
        projection = ','.join(['`%s` as _id' % c if c == 'id' else '`%s`' % c for c in projection])
        if sort:
            sort = 'order by ' + ','.join(['%s %s' % (one[0], ORDER.get(one[-1], 'asc')) for one in sort])
        else:
            sort = ''
        if update:
            update = 'for update'
        else:
            update = ''
        if where:
            where = 'where %s' % where
        if limit is None:
            L = dbpc.handler.queryAll('select %s from `%s` %s %s %s' % (projection, cls.__table__, where, sort, update), [args[index][one] for index, one in enumerate(keys)])
        else:
            L = dbpc.handler.queryAll('select %s from `%s` %s %s limit %d, %d %s' % (projection, cls.__table__, where, sort, skip, limit, update), [args[index][one] for index, one in enumerate(keys)])
        return L

    @classmethod
    def count(cls, spec):
        '''
        Find by 'select count(pk) from table where ... ' and return int.
        '''
        keys = []
        args = []
        rectify(cls, '_id', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        if where:
            where = 'where %s' % where
        return dbpc.handler.queryOne('select count(*) as total from `%s` %s' % (cls.__table__, where), [args[index][one] for index, one in enumerate(keys)])['total']

    @classmethod
    def insert(cls, obj, update=True, method='SINGLE', maxsize=CFG._BUFFER):
        if cls.__lock is None:
            cls.__lock = threading.Lock()
        if obj is not None:
            updatekeys = []
            for k, v in cls.__mappings__.iteritems():
                if not hasattr(obj, k) and not isinstance(v, IdField):
                    setattr(obj, k, v.default)
                if update:
                    if not v.primary and v.updatable:
                        updatekeys.append(k)

            tid = obj.pop('tid', None)
            if '_id' in obj:
                obj['id'] = obj.pop('_id')
            items = obj.items()
            items.sort(lambda x,y:cmp(x[0], y[0]))
            if tid:
                items.append(('tid', tid))
                obj['tid'] = tid

            if cls._insertsql is None or method == 'SINGLE':
                if update:
                    if 'id' in obj:
                        del obj['id']
                    cls._insertsql = 'insert into `%s` (%s) ' % (cls.__table__, ','.join('`'+one[0]+'`' for one in items)) + 'values (%s)' % ','.join('%s' for one in items) + ' on duplicate key update %s' % ','.join('`'+one+'`=values(`'+one+'`)' for one in updatekeys if not one == 'create_time')
                else:
                    cls._insertsql = 'insert ignore into `%s` (%s) ' % (cls.__table__, ','.join('`'+one[0]+'`' for one in items)) + 'values (%s)' % ','.join('%s' for one in items)
            one = tuple([i[1] for i in items])
        else:
            one = None
        if method == 'SINGLE':
            if one:
                try:
                    _id = dbpc.handler.insert(cls._insertsql, one, method)
                    dbpc.handler.commit()
                    return obj.get(cls.id_name, _id)
                except:
                    dbpc.handler.rollback()
                    raise
        else:
            with cls.__lock:
                if one is not None:
                    cls._insertdatas.append(one)
                if sys.getsizeof(cls._insertdatas) > maxsize or (cls.expire and (time.time() - cls._insertstamp) > cls.expire):
                    try:
                        dbpc.handler.insert(cls._insertsql, cls._insertdatas, method)
                        dbpc.handler.commit()
                    except:
                        dbpc.handler.rollback()
                        raise
                    finally:
                        cls._insertdatas = []
                        cls._insertstamp = time.time()

    @classmethod
    def delete(cls, spec):
        if spec == {}:
            raise Exception("Wrong delete spec.")
        keys = []
        args = []
        rectify(cls, '_id', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        dbpc.handler.delete('delete from `%s` where %s' % (cls.__table__, where), [args[index][one] for index, one in enumerate(keys)])

    @classmethod
    def update(cls, spec, doc):
        if spec == {}:
            raise Exception("Wrong update spec.")
        for k in doc:
            if not k in ('$set', '$inc'):
                raise Exception("Wrong update doc, only assist $set and $inc.")
        sets = doc.get('$set', {}).items()
        if sets:
            resets = [','.join('`'+one[0]+'`=%s' for one in sets)]
        else:
            resets = []
        incs = doc.get('$inc', {}).items()
        incs = ','.join('`%s`=`%s`+%d' % (one[0], one[0], one[1]) for one in incs)
        if incs:
            resets.append(incs)
        keys = []
        args = []
        rectify(cls, '_id', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        dbpc.handler.update('update `%s` set %s where %s' % (cls.__table__, ','.join(resets), where), [one[1] for one in sets] + [args[index][one] for index, one in enumerate(keys)])

    @classmethod
    def init_table(cls):
        doc = cls.genDoc()
        dbpc.handler.operate(doc)


if __name__=='__main__':
    pass
