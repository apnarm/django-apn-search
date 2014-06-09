#!/usr/bin/env python

from setuptools import setup

setup(
    name='django-apn-search',
    version='0.0.1',
    description='An extension of django-haystack that provides extra features.',
    author='Raymond Butcher',
    author_email='randomy@gmail.com',
    url='https://github.com/apn-online/django-apn-search',
    license='MIT',
    packages=(
        'apn_search',
    ),
    install_requires=(
        'django >= 1.2.0, < 1.3.0',
        'django-haystack == 2.0.0-beta-apn-online-0.4',
        'django-lazycache',
        'python-mq',
    ),
)
