#!/usr/bin/python
# coding=utf-8
import time, datetime, logging, threading, sys, traceback, hashlib
from suit import dbpc
from ..util import rectify, transfer
from .. import Field

MAXSIZE = 20

ORDER = {1:'asc', -1:'desc'}

try:
    from kokolog.aboutfile import modulename, modulepath
    from kokolog.prettyprint import logprint
except:
    def modulename(n):
        return None

    def modulepath(p):
        return None

    def logprint(n, p):
        def _wraper(*args, **kwargs):
            print(' '.join(args))
        return _wraper, None

_print, logger = logprint(modulename(__file__), modulepath(__file__))


class IdField(Field):

    def __init__(self, strict=False, **attributes):
        if not strict and not 'default' in attributes:
            attributes['default'] = 0
        attributes['ddl'] = '%s(%d)' % ('int', 11)
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


_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])

def genDoc(tablename, tablefields):
    pk = None
    uniques = {}
    doc = ['-- generating DOC for %s:' % tablename, 'create table if not exists `%s` (' % tablename]
    doc.append('`id` int(11) not null auto_increment,')
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
        doc.append('  primary key (`id`),')
        doc.append(',\n'.join('  unique key `%s` (%s)' % (key, ','.join('`'+one+'`' for one in val)) for key, val in uniques.items()))
    else:
        doc.append('  primary key (`id`)')
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
    _insertsql = None
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
    def queryOne(cls, spec, projection={}, sort=[]):
        '''
        Find by where clause and return one result. If multiple results found, 
        only the first one returned. If no result found, return None.
        '''
        keys = []
        args = []
        rectify(cls, IdField, 'IdField', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        if projection:
            projection['_id'] = projection.get('_id', 1)
            projection = ['id' if k == '_id' else k for k, v in projection.items() if v == 1]
        else:
            projection = cls.__mappings__.keys()
            projection.append('id')
        projection = ','.join(['`%s` as _id' % c if c == 'id' else '`%s`' % c for c in projection])
        if sort:
            sort = 'order by ' + ','.join(['%s %s' % (one[0], ORDER.get(one[-1], 'asc')) for one in sort])
        else:
            sort = ''
        d = dbpc.handler.queryOne('select %s from `%s` where %s %s limit %d, %d' % (projection, cls.__table__, where, sort, 0, 1), [args[index][one] for index, one in enumerate(keys)])
        return d

    @classmethod
    def queryAll(cls, spec, projection={}, sort=[], skip=0, limit=10):
        '''
        Find all and return list.
        '''
        keys = []
        args = []
        rectify(cls, IdField, 'IdField', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        if projection:
            projection['_id'] = projection.get('_id', 1)
            projection = ['id' if k == '_id' else k for k, v in projection.items() if v == 1]
        else:
            projection = cls.__mappings__.keys()
            projection.append('id')
        projection = ','.join(['`%s` as _id' % c if c == 'id' else '`%s`' % c for c in projection])
        if sort:
            sort = 'order by ' + ','.join(['%s %s' % (one[0], ORDER.get(one[-1], 'asc')) for one in sort])
        else:
            sort = ''
        if where:
            where = 'where %s' % where
        L = dbpc.handler.queryAll('select %s from `%s` %s %s limit %d, %d' % (projection, cls.__table__, where, sort, skip, limit), [args[index][one] for index, one in enumerate(keys)])
        return L

    @classmethod
    def count(cls, spec):
        '''
        Find by 'select count(pk) from table where ... ' and return int.
        '''
        keys = []
        args = []
        rectify(cls, IdField, 'IdField', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        if where:
            where = 'where %s' % where
        return dbpc.handler.queryOne('select count(*) as total from `%s` %s' % (cls.__table__, where), [args[index][one] for index, one in enumerate(keys)])['total']

    @classmethod
    def insert(cls, obj, update=True, method='SINGLE', forcexe=False, maxsize=MAXSIZE):
        if cls.__lock is None:
            cls.__lock = threading.Lock()
        if '_id' in obj:
            obj['id'] = obj['_id']
            del obj['_id']
        if obj is not None:
            updatekeys = []
            for k, v in obj.__mappings__.iteritems():
                if not hasattr(obj, k):
                    setattr(obj, k, v.default)
                if update:
                    if v.updatable:
                        updatekeys.append(k)
            items = obj.items()
            items.sort(lambda x,y:cmp(x[0], y[0]))
            if cls._insertsql is None or method == 'SINGLE':
                if update:
                    cls._insertsql = 'insert into `%s` (%s) ' % (cls.__table__, ','.join('`'+one[0]+'`' for one in items)) + 'values (%s)' % ','.join('%s' for one in items) + ' on duplicate key update %s' % ','.join('`'+one+'`=values(`'+one+'`)' for one in updatekeys if not one == 'create_time')
                else:
                    cls._insertsql = 'insert ignore into `%s` (%s) ' % (cls.__table__, ','.join('`'+one[0]+'`' for one in items)) + 'values (%s)' % ','.join('%s' for one in items)
            one = tuple([i[1] for i in items])
        else:
            one = None
        if method == 'SINGLE':
            if one:
                try:
                    dbpc.handler.insert(cls._insertsql, one, method)
                    dbpc.handler.commit()
                    return dbpc.handler.queryOne(""" select last_insert_id() as lastid; """)['lastid']
                except:
                    t, v, b = sys.exc_info()
                    err_messages = traceback.format_exception(t, v, b)
                    txt = ','.join(err_messages)
                    _print('db ', tid=obj.get(tid, ''), sname='', args='', kwargs='', txt=txt)
                    dbpc.handler.rollback()
        else:
            with cls.__lock:
                if one is not None:
                    cls._insertdatas.append(one)
                if forcexe:
                    try:
                        if cls._insertdatas:
                            dbpc.handler.insert(cls._insertsql, cls._insertdatas, method)
                            dbpc.handler.commit()
                            cls._insertdatas = []
                    except:
                        t, v, b = sys.exc_info()
                        err_messages = traceback.format_exception(t, v, b)
                        txt = ','.join(err_messages)
                        _print('db ', tid=cls._insertdatas[0].get(tid, ''), sname='', args='', kwargs='', txt=txt)
                        dbpc.handler.rollback()
                else:
                    if sys.getsizeof(cls._insertdatas) > maxsize:
                        try:
                            dbpc.handler.insert(cls._insertsql, cls._insertdatas, method)
                            dbpc.handler.commit()
                            cls._insertdatas = []
                        except:
                            t, v, b = sys.exc_info()
                            err_messages = traceback.format_exception(t, v, b)
                            txt = ','.join(err_messages)
                            _print('db ', tid=cls._insertdatas[0].get(tid, ''), sname='', args='', kwargs='', txt=txt)
                            dbpc.handler.rollback()

    @classmethod
    def delete(cls, spec):
        if spec == {}:
            raise Exception("Wrong delete spec.")
        keys = []
        args = []
        rectify(cls, IdField, 'IdField', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        return dbpc.handler.delete('delete from `%s` where %s' % (cls.__table__, where), [args[index][one] for index, one in enumerate(keys)])

    @classmethod
    def update(cls, spec, doc):
        if spec == {}:
            raise Exception("Wrong update spec.")
        if not '$set' in doc and not '$inc' in doc:
            raise Exception("Wrong update doc.")
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
        rectify(cls, IdField, 'IdField', spec)
        where = transfer(spec, grand=None, parent='', index=keys, condition=args)
        dbpc.handler.update('update `%s` set %s where %s' % (cls.__table__, ','.join(resets), where), [one[1] for one in sets] + [args[index][one] for index, one in enumerate(keys)])


if __name__=='__main__':
    pass
