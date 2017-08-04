==========
 workflow
==========

.. image:: https://travis-ci.org/inveniosoftware-contrib/workflow.png?branch=master
    :target: https://travis-ci.org/inveniosoftware-contrib/workflow
.. image:: https://coveralls.io/repos/github/inveniosoftware-contrib/workflow/badge.svg?branch=master
    :target: https://coveralls.io/github/inveniosoftware-contrib/workflow?branch=master

About
=====

Workflow is a Finite State Machine with memory.  It is used to execute
set of methods in a specified order.

Here is a simple example of a workflow configuration:

.. code-block:: text

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

Documentation
=============

Documentation is readable at http://workflow.readthedocs.io or can be built using Sphinx: ::

    pip install Sphinx
    python setup.py build_sphinx

Installation
============

Workflow is on PyPI so all you need is: ::

    pip install workflow

Testing
=======

Running the test suite is as simple as: ::

    python setup.py test

or, to also show code coverage: ::

    ./run-tests.sh
