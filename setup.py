# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2014, 2015, 2016, 2018 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

"""Simple workflows for Python"""

import os
import platform

from setuptools import find_packages, setup

readme = open('README.rst').read()

tests_require = [
    'coverage>=4.0',
    'mock>=1.0.0',
    'isort>=4.2.2',
    'pytest-cache>=1.0',
    'pytest-cov>=1.8.0',
    'pytest-pep8>=1.0.6',
    'pytest>=2.8.0',
]

extras_require = {
    'docs': [
        'Sphinx>=1.4.2',
    ],
    'tests': tests_require,
}

extras_require['all'] = []
for reqs in extras_require.values():
    extras_require['all'].extend(reqs)

install_requires = [
    'autosemver~=0.5',
    'configobj>4.7.0',
    'blinker>=1.3',
    'six',
]

setup_requires = [
    'autosemver~=0.5',
    'pytest-runner>=2.6.2',
]

if platform.python_version_tuple() < ('3', '4'):
    install_requires.append('enum34>=1.0.4')

packages = find_packages(exclude=['docs', 'tests'])

URL = 'https://github.com/inveniosoftware/workflow'

setup(
    name='workflow',
    description=__doc__,
    packages=packages,
    scripts=['bin/run_workflow.py'],
    author='Invenio Collaboration',
    author_email='info@inveniosoftware.org',
    url=URL,
    keywords=['workflows', 'finite state machine', 'task execution'],
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    extras_require=extras_require,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_require,
    autosemver={
        'bugtracker_url': URL + '/issues',
    },
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Development Status :: 5 - Production/Stable',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Utilities',
    ],
)
