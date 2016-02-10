#!/usr/bin/env python
from __future__ import print_function

import nose

from plugins import dtestconfig


class NullResult():
    def wasSuccessful(self):
        return True

class NullRunner():
    def run(self, test):
        return NullResult()

if __name__ == '__main__':
    spelunking_plugin = dtestconfig.DtestConfigPlugin()
    nose.core.TestProgram(
        testRunner=NullRunner(),
        addplugins=[spelunking_plugin],
        exit=False,
    )
    print(spelunking_plugin.CONFIG)

    # nose.main(addplugins=[testconfig.DtestConfigPlugin()])
