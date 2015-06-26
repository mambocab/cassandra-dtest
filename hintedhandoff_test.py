from dtest import Tester, debug, DISABLE_VNODES
from tools import require, create_c1c2_table, insert_c1c2, query_c1c2
from cassandra import ConsistencyLevel

@require("9035")
class TestHintedHandoff(Tester):

    def _start_two_node_cluster(self, config_options = None):
        """
        Start a cluster with two nodes and return them
        """
        cluster = self.cluster

        if config_options:
            cluster.set_configuration_options(values=config_options)

        if DISABLE_VNODES:
            cluster.populate([2]).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate([2], tokens=tokens).start()

        return cluster.nodelist()

    def _launch_nodetool_cmd(self, node, cmd):
        """
        Launch a nodetool command and check there is no error, return the result
        """
        out, err = node.nodetool(cmd, capture_output=True)
        self.assertEqual('', err)
        return out

    def _do_hinted_handoff(self, node1, node2, enabled):
        """
        Test that if we stop one node the other one
        will store hints only when hinted handoff is enabled
        """
        cursor = self.patient_exclusive_cql_connection(node1)
        self.create_ks(cursor, 'ks', 2)
        create_c1c2_table(self, cursor)

        node2.stop(wait_other_notice=True)

        for n in xrange(0, 100):
            insert_c1c2(cursor, n, ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start(wait_other_notice=True)

        if enabled:
            node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH if enabled or not if not enabled
        cursor = self.patient_exclusive_cql_connection(node2, keyspace='ks')
        for n in xrange(0, 100):
            if enabled:
                query_c1c2(cursor, n, ConsistencyLevel.ONE)
            else:
                query_c1c2(cursor, n, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    def nodetool_test(self):
        """
        Test various nodetool commands
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled' : True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

            self._launch_nodetool_cmd(node, 'disablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is not running', res.rstrip())

            self._launch_nodetool_cmd(node, 'enablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

            self._launch_nodetool_cmd(node, 'disablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running\nData center dc1 is disabled', res.rstrip())

            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

    def hintedhandoff_disabled_test(self):
        """
        Test gloabl hinted handoff disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled' : False})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is not running', res.rstrip())

        self._do_hinted_handoff(node1, node2, False)

    def hintedhandoff_enabled_test(self):
        """
        Test global hinted handoff enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled' : True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

        self._do_hinted_handoff(node1, node2, True)

    def hintedhandoff_dc_disabled_test(self):
        """
        Test global hinted handoff enabled with the dc disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled' : True,
                                                     'hinted_handoff_disabled_datacenters' : ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running\nData center dc1 is disabled', res.rstrip())

        self._do_hinted_handoff(node1, node2, False)


    def hintedhandoff_dc_reenabled_test(self):
        """
        Test global hinted handoff enabled with the dc disabled first and then re-enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled' : True,
                                                     'hinted_handoff_disabled_datacenters' : ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running\nData center dc1 is disabled', res.rstrip())

        for node in node1, node2:
            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

        self._do_hinted_handoff(node1, node2, True)