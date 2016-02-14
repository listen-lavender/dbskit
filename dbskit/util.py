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
                    operator = explain(k)
                    if operator is not None:
                        k = parent
                    operator = operator or '='
                    k = 'id' if k == '_id' else k
                    if v is None:
                        multi.append('(`' + k + '` is null)')
                    else:
                        index.append(k)
                        condition.append({k:v})
                        multi.append('(`' + k + '`' + operator + '%s' + ')')
            return '(' + ' and '.join(multi) + ')'
        else:
            return ''

if __name__ == '__main__':
    spec = {'username':'haokuan@adesk.com', 'password':'123456', 'status':{'$ne':0}}
    index = []
    condition = []
    print transfer(spec, grand=None, parent='', index=[], condition=[])
    