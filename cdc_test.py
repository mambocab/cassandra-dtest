from __future__ import division

from cassandra.concurrent import execute_concurrent_with_args

from dtest import Tester
from tools import rows_to_list


"""
- We want to add dtests to determine that CDC data is correctly flushed to
  cdc_raw and not discarded. Our basic goal is to write dataset A to CDC tables
  and dataset B to non-CDC tables, flush, then check that a superset of data A
  is available to a client in cdc_raw. We can do this by:
    - writing a mixed CDC/non-CDC dataset, tracking all data that should be
      exposed via CDC,
    - flushing,
    - saving off the contents of cdc_raw,
    - shutting down the cluster,
    - starting a new cluster,
    - initializing schema as necessary,
    - shutting down the new cluster,
    - moving the saved cdc_raw contents to commitlog directories,
    - starting the new cluster to commitlog replay, then
    - asserting all data that should have been exposed via CDC is in the
      table(s) that have been written to tables in the new cluster.
- We need tests of commitlog discard behavior when cdc_raw is full. In
  particular:
- once it reaches its maximum configured size, CDC tables should start
  rejecting writes, while CDC tables should still accept them.
- any new commitlog segments written after cdc_raw has reached its maximum
  configured size should be deleted, not moved to cdc_raw, on commitlog
  discard.
"""


class TestCDC(Tester):
    """
    @jira_ticket CASSANDRA-8844

    Test the correctness of some features of CDC, Change Data Capture, which
    provides an view of the commitlog on tables for which it is enabled.
    """

    def test_tables_readable_round_trip(self):
        """
        - We need basic tests to determine that commitlogs written with CDC enabled
          can be read when the CDC table has CDC disabled, and vice-versa.
        """
        self.cluster.populate(1).set_configuration_options({'cdc_enabled': True}).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        ks_name, table_name = 'ks', 'tab'
        self.create_ks(session, ks_name, rf=1)
        session.execute('CREATE TABLE ' + table_name + ' (a int PRIMARY KEY, b int) WITH CDC = true;')
        insert_stmt = session.prepare('INSERT INTO ' + table_name + ' (a, b) VALUES (?, ?)')

        data = tuple(zip(tuple(range(1000)), tuple(range(1000))))

        execute_concurrent_with_args(session, insert_stmt, data)

        # We need data to be in commitlogs, not sstables.
        self.assertEqual([], list(node.get_sstables(ks_name, table_name)))

        session.execute('ALTER TABLE ' + table_name + ' WITH CDC = false;')

        self.assertItemsEqual(
            rows_to_list(session.execute('SELECT * FROM ' + table_name)),
            data
        )
