#!/usr/bin/python
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

    def update(self, spec, doc, db=None, collection=None, upsert=False, method='SINGLE'):
        db = db or self.db
        for key in doc:
            if not '$' in key:
                raise " Update document must be start with $. "
        multi = not method.upper() == 'SINGLE'
        return self._conn[db][collection].update(spec, doc, upsert=upsert, multi=multi)

    def delete(self, spec, db=None, collection=None, method='SINGLE'):
        db = db or self.db
        multi = not method.upper() == 'SINGLE'
        return self._conn[db][collection].remove(spec, multi=multi)

    def insert(self, doc, db=None, collection=None, method='SINGLE', lastid=None):
        db = db or self.db
        if method == 'SINGLE':
            if not isinstance(doc, dict):
                raise "Single insert document must be dict type."
            # return self._conn[db][collection].insert_one(doc).inserted_id
        else:
            if not type(doc) == list:
                raise "Bulk insert document must be list type."
            # return self._conn[db][collection].insert_many(doc).inserted_ids
        return self._conn[db][collection].insert(doc)

    def showColumns(self, table):
        """
            查看表的列
            @param table: 表名称
            @return columns: 列名
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
