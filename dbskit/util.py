#!/usr/bin/env python
# coding=utf-8

def explain(desc):
    d = {'$gt':' > ',
        '$lt':' < ',
        '$gte':' >= ',
        '$lte':' <= ',
        '$ne':' <> ',
        '$in':' in ',
        '$nin':' not in ',
        '$or':' or ',
        '$and':' and ',
        '':' and ',
        '$regex': ' like ',
        '$mod':' mod ',
    }
    return d.get(desc)

def transfer(spec={}, grand=None, parent='', index=[], condition=[]):
    """
        递归转换mongo查询为mysql查询
    """
    if isinstance(spec, list):
        multi = []
        for one in spec:
            if isinstance(one, dict):
                multi.append(transfer(one, grand=parent, parent='', index=index, condition=condition))
            else:
                index.append(grand)
                condition.append({grand:one})
        operator = explain(parent)
        if multi:
            return '(' + operator.join(multi) + ')'
        else:
            grand = 'id' if grand == '_id' else grand
            if operator.strip() == 'mod':
                return '(`' + grand + '` %s' + operator + '%s)'
            else:
                return '(`' + grand + '` in (' + ','.join(['%s' for k in spec]) + '))'

    elif isinstance(spec, dict):
        multi = []
        for k, v in spec.items():
            if isinstance(v, dict):
                multi.append(transfer(v, grand=parent, parent=k, index=index, condition=condition))
            elif isinstance(v, list):
                multi.append(transfer(v, grand=parent, parent=k, index=index, condition=condition))
            else:
                if k == '$options':
                    continue
                operator = explain(k)
                if operator is not None:
                    k = parent
                operator = operator or '='
                k = 'id' if k == '_id' else k
                if v is None:
                    multi.append('(`' + k + '` is null)')
                elif k == '' or k is None:
                    raise Exception("Empty string key or None key.")
                    # multi.append('("" = "")')
                else:
                    index.append(k)
                    if ' like ' == operator:
                        if v.startswith('^'):
                            v = v[1:] + '%'
                        elif v.endswith('$'):
                            v = '%' + v[:-1]
                        else:
                            v = '%' + v + '%'
                    condition.append({k:v})
                    multi.append('(`' + k + '`' + operator + '%s' + ')')
        return '(' + ' and '.join(multi) + ')' if multi else ''
    else:
        return ''


def rectify(cls, field, spec={}, grand=None, parent=''):
    """
        递归检查类型
    """
    if isinstance(spec, list):
        for index, one in enumerate(spec):
            if isinstance(one, dict):
                rectify(cls, field, one, grand=parent, parent='')
            else:
                if one is None:
                    continue
                if grand in cls.__mappings__:
                    spec[index] = cls.__mappings__[grand].verify(one)

    elif isinstance(spec, dict):
        for k, v in spec.items():
            if isinstance(v, dict):
                rectify(cls, field, v, grand=parent, parent=k)
            elif isinstance(v, list):
                rectify(cls, field, v, grand=parent, parent=k)
            else:
                operator = explain(k)
                if operator is not None:
                    f = parent
                else:
                    f = k
                if v is None:
                    continue
                if f in cls.__mappings__:
                    spec[k] = cls.__mappings__[f].verify(spec[k])
    else:
        pass


if __name__ == '__main__':
    spec = {'username':'haokuan@adesk.com', 'password':'123456', 'status':{'$ne':0}}
    spec = {'$or':[{'uid':{'$regex':'a$', '$options':'i'}}, {'a':''}]}
    index = []
    condition = []
    print transfer(spec, grand=None, parent='', index=index, condition=condition)
    print condition
    print index
    