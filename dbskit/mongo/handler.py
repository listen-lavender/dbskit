#!/usr/bin/env python
# coding=utf-8

import pymongo as dblib

class DBHandler(object):
    def __init__(self, markname, conn, check=(lambda tips, data:data), resutype='DICT', autocommit=False, db=''):
        self._markname = markname
        self._conn = conn
        self._check = check
        self._resutype = {'TUPLE':'TUPLE', 'DICT':'DICT'}[resutype]
        self.db = db

    @classmethod
    def wrap(cls, tpl):
        if isinstance(doc, dict):
            tpl['tips'] = tpl.get('tips')
        else:
            tpl = {'collection':tpl, 'tips':None}
        return tpl
    
    def check(self, tips, data):
    	return self._check(tips, data)

    def queryAll(self, spec, db=None, collection=None, **kwargs):
        db = db or self.db
        return self.query(spec, db=db, collection=collection, qt='all', **kwargs)

    def queryOne(self, spec, db=None, collection=None, **kwargs):
        db = db or self.db
        return self.query(spec, db=db, collection=collection, qt='one', **kwargs)

    def query(self, spec, db=None, collection=None, qt='all', **kwargs):
        db = db or self.db
        if qt.lower() == 'one':
            return self._conn[db][collection].find_one(spec, **kwargs)
        else:
            return self._conn[db][collection].find(spec, **kwargs)

    def update(self, spec, doc, batch=None, db=None, collection=None, upsert=False, method='SINGLE'):
        db = db or self.db
        if method.upper() == 'SINGLE':
            if not isinstance(spec, dict) or not isinstance(doc, dict):
                raise "Single update condition and document must be dict type."
            self._conn[db][collection].update(spec, doc, upsert=upsert, multi=False)
            rows = 1
        else:
            if not isinstance(batch, list):
                raise "Bulk update condition and document must be list type."
            bulk = self._conn[db][collection].initialize_ordered_bulk_op()
            for spec, doc in batch:
                buff = bulk.find(spec)
                if upsert:
                    buff.upsert().update_one(doc)
                else:
                    buff.update_one(doc)
            bulk.execute()
            del bulk
            rows = len(batch)
        return rows

    def delete(self, spec, db=None, collection=None, method='SINGLE'):
        db = db or self.db
        if method.upper() == 'SINGLE':
            if not isinstance(spec, dict):
                raise "Condition must be dict type."
            rows = self._conn[db][collection].remove(spec)
        else:
            if not isinstance(spec, list):
                raise "Bulk remove condition must be list type."
            bulk = self._conn[db][collection].initialize_ordered_bulk_op()
            for one in spec:
                buff = bulk.find(one)
                buff.remove()
            bulk.excute()
            del bulk
            rows = len(doc)
        return rows

    def insert(self, doc, db=None, collection=None, method='SINGLE', update=False, lastid=None):
        if update:
            if method.upper() == 'SINGLE':
                spec, doc = doc
                return self.update(spec, doc, db=db, collection=collection, upsert=True, method=method)
            else:
                return self.update(None, None, batch=doc, db=db, collection=collection, upsert=True, method=method)
        db = db or self.db
        if method.upper() == 'SINGLE':
            if not isinstance(doc, dict):
                raise "Single insert document must be dict type."
            lastid = self._conn[db][collection].insert_one(doc).inserted_id
        else:
            if not isinstance(doc, list):
                raise "Bulk insert document must be list type."
            try:
                self._conn[db][collection].insert_many(doc, ordered=False)
            except:
                pass
        return lastid

    def orderedBulk(self, db, collection):
        self.bulk_cache = {}
        if db is None or collection is None:
            raise "Db or collection is None."
        key = '%s-%s-%s' % (self._markname, db, collection)
        val = self.bulk_cache.get(key)
        if val is None:
            val = self._conn[db][collection].initialize_ordered_bulk_op()
            self.bulk_cache[key] = val
        return val

    def showColumns(self, table):
        """
        """
        sql = """ select `column_name`, `data_type`
                    from information_schema.columns
                where `table_schema` = %s and `table_name`=%s
        """
        columns = {}
        tables = self._conn[self.db][table].find_one()
        if tables:
            for key, val in tables.items():
                columns[key] = type(val)
        return columns

class ExampleDBHandler(DBHandler):
    pass
