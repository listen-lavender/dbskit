#!/usr/bin/python
# coding=utf-8

"""
    命名空间的内容初始化
"""

__import__('pkg_resources').declare_namespace(__name__)
__version__ = '0.0.3'
__author__ = 'hk'

def singleton(cls):
    instances = {}
    def _singleton(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return _singleton
