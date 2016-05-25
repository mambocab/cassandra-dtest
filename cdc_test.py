from __future__ import division

from cassandra.concurrent import execute_concurrent_with_args

from dtest import Tester, debug
from functools import partial


def set_cdc_on_table(session, table_name, value, ks_name=None):
    """
    Uses <session> to set CDC to <value> on <ks_name>.<table_name>.
    """
    table_string = ks_name + '.' + table_name if ks_name else table_name
    value_string = 'true' if value else 'false'
    stmt = 'ALTER TABLE ' + table_string + ' WITH CDC = ' + value_string

    debug(stmt)
    session.execute(stmt)


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

def enable_and_disable_cdc_funcs_for(session, ks_name, table_name):
    """
    Generate {enable,disable}_cdc functions that use session to enable or
    disable CDC on <ks_name>.<table_name>.

    <ks_name> is a required argument for this function even though it could be
    optional in the case where <session> has USEd a keyspace. This is because
    this function closes over <session>, and if other code has <session> USE a
    different keyspace, it would change the behavior of the functions returned
    here.
    """
    enable_cdc = partial(set_cdc_on_table,
                         session=session,
                         ks_name=ks_name, table_name=table_name,
                         value=True)
    disable_cdc = partial(set_cdc_on_table,
                          session=session,
                          ks_name=ks_name, table_name=table_name,
                          value=False)
    return enable_cdc, disable_cdc

class TestCDC(Tester):
    """
    @jira_ticket CASSANDRA-8844

    Test the correctness of some features of CDC, Change Data Capture, which
    provides an view of the commitlog on tables for which it is enabled.
    """

    def prepare(self, ks_name):
        self.cluster.populate(1).set_configuration_options({'cdc_enabled': True}).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_ks(session, ks_name, rf=1)
        return node, session

    def test_cdc_data_tables_readable_round_trip_enabled(self):
        """
        Ensure that data written to a CDC-enabled table is still readable after
        disabling CDC on that table, then again after reenabling it.
        """
        ks_name, table_name = 'ks', 'tab'
        node, session = self.prepare(ks_name=ks_name)
        session.execute('CREATE TABLE ' + table_name + ' (a int PRIMARY KEY, b int) WITH CDC = true')
        insert_stmt = session.prepare('INSERT INTO ' + table_name + ' (a, b) VALUES (?, ?)')

        data = tuple(zip(list(range(1000)), list(range(1000))))
        execute_concurrent_with_args(session, insert_stmt, data)

        # We need data to be in commitlogs, not sstables.
        self.assertEqual([], list(node.get_sstables(ks_name, table_name)))

        enable_cdc, disable_cdc = enable_and_disable_cdc_funcs_for(session=session, ks_name=ks_name, table_name=table_name)

        disable_cdc()
        self.assertItemsEqual(session.execute('SELECT * FROM ' + table_name), data)
        enable_cdc()
        self.assertItemsEqual(session.execute('SELECT * FROM ' + table_name), data)

    def test_cdc_data_tables_readable_round_trip_disabled(self):
        """
        Ensure that data written to a CDC-enabled table is still readable after
        disabling CDC on that table, then again after reenabling it.
        """
        ks_name, table_name = 'ks', 'tab'
        node, session = self.prepare(ks_name=ks_name)
        session.execute('CREATE TABLE ' + table_name + ' (a int PRIMARY KEY, b int) WITH CDC = false')
        insert_stmt = session.prepare('INSERT INTO ' + table_name + ' (a, b) VALUES (?, ?)')

        data = tuple(zip(list(range(1000)), list(range(1000))))
        execute_concurrent_with_args(session, insert_stmt, data)

        # We need data to be in commitlogs, not sstables.
        self.assertEqual([], list(node.get_sstables(ks_name, table_name)))

        enable_cdc, disable_cdc = enable_and_disable_cdc_funcs_for(session=session, ks_name=ks_name, table_name=table_name)

        enable_cdc()
        self.assertItemsEqual(session.execute('SELECT * FROM ' + table_name), data)
        disable_cdc()
        self.assertItemsEqual(session.execute('SELECT * FROM ' + table_name), data)
