from __future__ import division

import os
import time

from cassandra.concurrent import execute_concurrent_with_args
from cassandra import WriteFailure

from dtest import Tester, debug
from itertools import izip as zip

from uuid import uuid4

CURRENT_DIR = os.path.abspath('.')
full_cdc_test_cache_name = 'full_cdc_cache'
SAVED_CDC_COMMITLOGS = {
    full_cdc_test_cache_name:
        os.path.join(CURRENT_DIR, full_cdc_test_cache_name, 'commitlogs'),
}


def set_cdc_on_table(session, table_name, value, ks_name=None):
    """
    Uses <session> to set CDC to <value> on <ks_name>.<table_name>.
    """
    table_string = ks_name + '.' + table_name if ks_name else table_name
    value_string = 'true' if value else 'false'
    stmt = 'ALTER TABLE ' + table_string + ' WITH CDC = ' + value_string

    debug(stmt)
    session.execute(stmt)


def get_set_cdc_func(session, ks_name, table_name):
    def f(value):
        return set_cdc_on_table(
            session=session,
            ks_name=ks_name, table_name=table_name,
            value=value
        )
    return f


def size_of_files_in_dir(dir_name):
    return sum(os.path.getsize(f) for f in os.listdir(dir_name) if os.path.isfile(f))

# def get_enable_and_disable_cdc_funcs(session, ks_name, table_name):
#     """
#     Generate {enable,disable}_cdc functions that use session to enable or
#     disable CDC on <ks_name>.<table_name>.

#     <ks_name> is a required argument for this function even though it could be
#     optional in the case where <session> has USEd a keyspace. This is because
#     this function closes over <session>, and if other code has <session> USE a
#     different keyspace, it would change the behavior of the functions returned
#     here.
#     """
#     enable_cdc = partial(set_cdc_on_table,
#                          session=session,
#                          ks_name=ks_name, table_name=table_name,
#                          value=True)
#     disable_cdc = partial(set_cdc_on_table,
#                           session=session,
#                           ks_name=ks_name, table_name=table_name,
#                           value=False)
#     return enable_cdc, disable_cdc


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
"""


class TestCDC(Tester):
    """
    @jira_ticket CASSANDRA-8844

    Test the correctness of some features of CDC, Change Data Capture, which
    provides an view of the commitlog on tables for which it is enabled.
    """

    def prepare(self, ks_name,
                table_name=None, cdc_enabled_table=None,
                data_schema=None,
                configuration_overrides=None):
        """
        Create a cluster, start it, create a keyspace, and if <table_name>,
        create a table in that keyspace. If <cdc_enabled_table>, that table is
        created with CDC enabled.
        """
        config_defaults = {'cdc_enabled': True}
        if configuration_overrides is None:
            configuration_overrides = {}
        self.cluster.populate(1)
        self.cluster.set_configuration_options(dict(config_defaults, **configuration_overrides))
        self.cluster.start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)
        self.create_ks(session, ks_name, rf=1)

        if table_name is not None:
            self.assertIsNotNone(cdc_enabled_table, 'if creating a table in prepare, must specify whether or not CDC is enabled on it')
            self.assertIsNotNone(data_schema, 'if creating a table in prepare, must specify its schema')
            stmt = ('CREATE TABLE ' + table_name +
                    ' ' + data_schema + ' ' +
                    'WITH CDC = ' + ('true' if cdc_enabled_table else 'false'))
            debug(stmt)
            session.execute(stmt)

        return node, session

    def _cdc_data_readable_on_round_trip(self, start_enabled):
        ks_name, table_name = 'ks', 'tab'
        sequence = [True, False, True] if start_enabled else [False, True, False]
        start, alter_path = sequence[0], list(sequence[1:])

        node, session = self.prepare(ks_name=ks_name, table_name=table_name,
                                     cdc_enabled_table=start,
                                     data_schema='(a int PRIMARY KEY, b int)')
        set_cdc = get_set_cdc_func(session=session, ks_name=ks_name, table_name=table_name)

        insert_stmt = session.prepare('INSERT INTO ' + table_name + ' (a, b) VALUES (?, ?)')
        data = tuple(zip(list(range(1000)), list(range(1000))))
        execute_concurrent_with_args(session, insert_stmt, data)

        # We need data to be in commitlogs, not sstables.
        self.assertEqual([], list(node.get_sstables(ks_name, table_name)))

        for x in alter_path:
            set_cdc(x)
            self.assertItemsEqual(session.execute('SELECT * FROM ' + table_name), data)

    def test_cdc_enabled_data_readable_on_round_trip(self):
        self._cdc_data_readable_on_round_trip(True)

    def test_cdc_disabled_data_readable_on_round_trip(self):
        self._cdc_data_readable_on_round_trip(False)

    def test_cdc_changes_rejected_on_full_cdc_log_dir(self):
        ks_name, full_cdc_table_name = 'ks', 'full_cdc_tab'

        # We're making assertions about CDC tables that fill up their
        # designated space, so we make cdc space as small as possible.
        configuration_overrides = {'cdc_total_space_in_mb': 1}
        node, session = self.prepare(
            ks_name=ks_name,
            table_name=full_cdc_table_name, cdc_enabled_table=True,
            data_schema='(a uuid PRIMARY KEY, b uuid)',
            configuration_overrides=configuration_overrides
        )
        insert_stmt = session.prepare('INSERT INTO ' + ks_name + '.' + full_cdc_table_name + ' (a, b) VALUES (?, ?)')

        # Later, we'll also make assertions about the behavior of non-CDC tables, so
        # we create one here.
        non_cdc_table_name = 'non_cdc_tab'
        session.execute('CREATE TABLE ' + ks_name + '.' + non_cdc_table_name +
                        ' (a uuid PRIMARY KEY, b uuid)')
        # We'll also make assertions about the behavior of CDC tables when
        # other CDC tables have already filled the designated space for CDC
        # commitlogs, so we create the second table here.
        emtpy_cdc_table_name = 'empty_cdc_tab'
        session.execute('CREATE TABLE ' + ks_name + '.' + emtpy_cdc_table_name +
                        ' (a uuid PRIMARY KEY, b uuid)')

        # Before inserting data, we flush to make sure that commitlogs from tables other than the
        # tables under test are gone.
        node.flush()
        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        self.assertEqual(0, size_of_files_in_dir(commitlog_dir))

        full_cdc_cache = SAVED_CDC_COMMITLOGS[full_cdc_test_cache_name]
        if os.path.exists(full_cdc_test_cache_name):
            for f in os.listdir(full_cdc_cache):
                os.symlink(os.path.join(full_cdc_cache, os.path.basename(f)), f)
        else:
            # Here, we insert values into the first CDC table until we get a
            # WriteFailure. This should happen when the CDC commitlogs take up
            # 1MB or more.
            start, printed_times, rows_loaded = time.time(), set(), 0
            debug('beginning data insert to fill CDC commitlogs')
            while True:
                seconds = time.time() - start
                self.assertLessEqual(
                    seconds, 600,
                    "It's taken more than 10 minutes to reach a WriteFailure trying "
                    'to overrun the space designated for CDC commitlogs. This could '
                    "be because data isn't being written quickly enough in this "
                    'environment, or because C* is failing to reject writes when '
                    'it should.'
                )

                int_seconds = int(seconds)
                if int_seconds % 10 == 0:
                    if int_seconds not in printed_times:
                        debug('  data load step has lasted {s:.2f}s, '
                              'loaded {r} rows'.format(s=seconds, r=rows_loaded))
                        printed_times.add(int_seconds)

                try:
                    session.execute(insert_stmt, (uuid4(), uuid4()))
                    rows_loaded += 1
                except WriteFailure:
                    debug("write failed (presumably because we've overrun "
                          "designated CDC commitlog space) after "
                          "{s:.2f}s".format(s=time.time() - start))

                    debug('Caching commitlogs in ' + full_cdc_cache)
                    for f in os.listdir(commitlog_dir):
                        os.symlink(f, os.path.join(full_cdc_cache, os.path.basename(f)))

                    break

        self.assertGreaterEqual(1024 ** 2, size_of_files_in_dir(commitlog_dir))

        debug(list(os.walk(commitlog_dir)))


"""
- We need tests of commitlog discard behavior when cdc_raw is full. In
  particular:
    - once it reaches its maximum configured size, CDC tables should start
      rejecting writes, while CDC tables should still accept them.
    - any new commitlog segments written after cdc_raw has reached its maximum
      configured size should be deleted, not moved to cdc_raw, on commitlog
      discard.
"""
