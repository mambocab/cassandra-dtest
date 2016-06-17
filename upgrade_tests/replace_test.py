from tools import since
from replace_address_test import BaseReplaceAddressTest


@since('2.2', max_version='2.9999')
class TestUpgradeReplaceAddress(BaseReplaceAddressTest):
    __test__ = True

    def insert_data_during_replace_mixed_versions_test(self):
        """
        Test that new replace procedure works with mixed cluster and replacement node
        does NOT receive writes during replacement (backward compatibility test)
        @jira_ticket CASSANDRA-8523
        """
        self._test_insert_data_during_replace(same_address=False, mixed_versions=True)
