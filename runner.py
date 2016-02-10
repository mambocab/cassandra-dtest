#!/usr/bin/env python

from nose import main

from plugins import testconfig


if __name__ == '__main__':
    main(addplugins=[testconfig.DtestConfigPlugin()])
