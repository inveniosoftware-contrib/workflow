# -*- coding: utf-8 -*-
#
# This file is part of Workflow.
# Copyright (C) 2014 CERN.
#
# Workflow is free software; you can redistribute it and/or modify it
# under the terms of the Revised BSD License; see COPYING.txt file for
# more details.

import unittest
import sys
import os

from workflow.config import config_reader


class TestConfig(unittest.TestCase):

    """Tests of the WE interface"""

    def setUp(self):
        config_reader.setBasedir(os.path.dirname(__file__))

    def tearDown(self):
        pass

    def test_basics(self):
        config_reader.init(os.path.join(os.path.dirname(__file__),
                                        'local.ini'))
        self.assertEqual(config_reader.STRING, 'string')
        self.assertEqual(config_reader.ARRAY, ['one', 'two', 'three'])
        self.assertEqual(config_reader.OVERRIDEN, 'local')
        self.assertEqual(config_reader.string, 'global/local')

        config_reader.init(os.path.join(os.path.dirname(__file__),
                                        'local2.ini'))
        self.assertEqual(config_reader.STRING, 'string')
        self.assertEqual(config_reader.ARRAY, ['one', 'two', 'three'])
        self.assertEqual(config_reader.OVERRIDEN, 'global')
        self.assertEqual(config_reader.string, 'second')


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestConfig))
    return suite

if __name__ == '__main__':
    unittest.main()
    # unittest.TextTestRunner(verbosity=2).run(suite())
