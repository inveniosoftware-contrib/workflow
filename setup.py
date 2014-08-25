# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2011, 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see COPYING.txt file for
# more details.

try:
    from setuptools import setup
except:
    from distutils.core import setup

import os
import re

# Get the version string.  Cannot be done with import!
with open(os.path.join('workflow', 'version.py'), 'rt') as f:
    version = re.search(
        '__version__\s*=\s*"(?P<version>.*)"\n',
        f.read()
    ).group('version')

setup(
    name='workflow',
    packages=['workflow', 'workflow.patterns'],
    scripts=['bin/run_workflow.py'],
    version=version,
    description='Simple workflows for Python',
    author='Roman Chyla',
    url='https://github.com/romanchyla/workflow',
    keywords=['workflows', 'finite state machine', 'task execution'],
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Development Status :: 5 - Production/Stable',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries :: Application Frameworks',
        'Topic :: Utilities',
    ],
    test_suite='nose.collector',
    tests_require=['nose', 'cloud', 'coverage'],
    install_requires=['configobj>4.7.0'],
    long_description="""\
Simple workflows for Python
-------------------------------------

Wofklow engine is a Finite State Machine with memory
It is used to execute set of methods in a specified order.

Here is a simple example of a configuration:

    [
      check_token_is_wanted, # (run always)
      [                      # (run conditionally)
         check_token_numeric,
         translate_numeric,
         next_token          # (stop processing, continue with next token)
         ],
      [                      # (run conditionally)
         check_token_proper_name,
         translate_proper_name,
         next_token          # (stop processing, continue with next token)
         ],
      normalize_token,       # (only for "normal" tokens)
      translate_token,
    ]

You can probably guess what the processing pipeline does with tokens - the
whole task is made of four steps and the whole configuration is just stored as
a Python list. Every task is implemeted as a function that takes two objects:

   * currently processed object
   * workflow engine instance

Example:

def next_token(obj, eng):
    eng.ContinueNextToken()

There are NO explicit states, conditions, transitions - the job of the engine
is simply to run the tasks one after another. It is the responsibility of the
task to tell the engine what is going to happen next; whether to continue,
stop, jump back, jump forward and few other options.

This is actually a *feature*, I knew that there will be a lot of possible
exceptions and transition states to implement for NLP processing and I also
wanted to make the workflow engine simple and fast -- but it has disadvantages,
you can make more errors and workflow engine will not warn you.

The workflow module comes with many patterns that can be directly used in the
definition of the pipeline, such as IF, IF_NOT, PARALLEL_SPLIT and others.

This version requires Python 2 and many of the workflow patterns (such as IF,
XOR, WHILE) are implemented using lambdas, therefore not suitable for Python 3.
"""
)
