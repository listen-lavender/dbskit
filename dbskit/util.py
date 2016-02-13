#!/usr/bin/python
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
        '$mod':' mod ',
    }
    return d.get(desc)

def transfer(spec={}, grand=None, parent='', index=[], condition=[]):
    """
        递归转换mongo查询为mysql查询
    """
    if spec == {}:
        return ''
    else:
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
                    operator = explain(k)
                    if explain(k) is None:
                        index.append(k)
                        condition.append({k:v})
                        multi.append('(`' + k + '`=%s' + ')')
                    else:
                        index.append(parent)
                        condition.append({parent:v})
                        multi.append('(`' + parent + '`' + operator + '%s' + ')')
            return '(' + ' and '.join(multi) + ')'
        else:
            return ''