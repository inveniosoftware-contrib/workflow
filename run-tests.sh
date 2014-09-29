#!/bin/sh
#
# This file is part of Workflow.
# Copyright (C) 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see LICENSE file for
# more details.

sphinx-build -qnNW docs docs/_build/html
python setup.py test
