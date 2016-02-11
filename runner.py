#!/usr/bin/env python
"""
Usage: runner.py [--nose-option NOSE_OPTIONS] [TESTS...]

--nose-option NOSE_OPTIONS  specify options to pass to `nosetests`.
TESTS                       space-separated list of tests to pass to `nosetests`
"""
from __future__ import print_function

import nose
from docopt import docopt

from plugins import dtestconfig


class NullResult():
    def wasSuccessful(self):
        return True

class NullRunner():
    def run(self, test):
        return NullResult()

def validate_and_serialize_options():
    pass

if __name__ == '__main__':
    options = docopt(__doc__)

    nose_option_list = options['--nose-option'].split()
    test_list = options['TESTS']
    nose_argv = nose_option_list + test_list

    nose.main(addplugins=[dtestconfig.DtestConfigPlugin()],
              argv=nose_argv)
