#!/usr/bin/env python
# coding=utf8

"""
    安装包工具
"""

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

install_requires = [
    'MySQL-python==1.2.5',
    'pymongo>=3.0.3',
    ]

dbskit = __import__('dbskit')
setup(name='dbskit',
version=dbskit.__version__,
description='wecatch dbskit',
author='haokuan',
author_email='jingdaohao@gmail.com',
url='https://github.com/listen-lavender/dbskit',
keywords='wecatch > ',
packages=find_packages(),
install_requires=install_requires,
)

