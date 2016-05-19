from __future__ import division
from dtest import Tester, debug
from itertools import product
from nose.tools import assert_is_not_none


class CDCMixin(Tester):

    __test__ = False
    cdc_enabled_node = cdc_enabled_tables = None

    def setUp(self):
        # It'd be nice to enforce this at class-generation time, but nose is
        # really bad at reporting errors in those cases, so we make our
        # assertions at test execution time :(
        assert_is_not_none(self.cdc_enabled_node)
        assert_is_not_none(self.cdc_enabled_tables)
        super(CDCMixin, self).setUp()


class TestCDCDataIntegrity(CDCMixin):

    __test__ = True
    cdc_enabled_node = cdc_enabled_tables = True

    def test_execute_this_test(self):
        pass

cdc_enabled_node_values = True, False
cdc_enabled_table__values = True, False

for cdc_enabled_node, cdc_enabled_tables, klaus in product(cdc_enabled_node_values,
                                                           cdc_enabled_table__values,
                                                           CDCMixin.__subclasses__()):
    debug(cdc_enabled_node)
    debug(cdc_enabled_table__values)
    debug(klaus)
