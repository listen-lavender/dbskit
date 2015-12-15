#!/usr/bin/python
# coding=utf8

"""
    安装包工具
"""

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

requires = [
    'mysql-connector-python==1.2.3',
    'MySQL-python==1.2.5',
    'pymongo>=3.0.3',
    ]

datakit = __import__('datakit')
setup(name='datakit',
version=datakit.__version__,
description='wecatch datakit',
long_description='',
author='haokuan',
author_email='jingdaohao@gmail.com',
url='http://www.google.com',
keywords='wecatch > ',
packages=find_packages(),
# package_data={'':['*.js', '*.css']},
namespace_packages=['datakit',],
include_package_data=True,
zip_safe=False,
install_requires=requires,
entry_points="",
scripts=[],
)

