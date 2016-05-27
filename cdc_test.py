from __future__ import division

import os
import time

from cassandra.concurrent import execute_concurrent_with_args, execute_concurrent
from cassandra import WriteFailure

from dtest import Tester, debug
from itertools import izip as zip


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
    files = [os.path.join(dir_name, f) for f in os.listdir(dir_name)]
    debug('getting sizes of these files: {}'.format(files))
    return sum(os.path.getsize(f) for f in files)

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
        config_defaults = {
            'cdc_enabled': True,
        }
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
                    ' ' + data_schema + ' '
                    'WITH CDC = ' + ('true' if cdc_enabled_table else 'false')
                    )
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
        insert_stmt = session.prepare('INSERT INTO ' + ks_name + '.' + full_cdc_table_name + ' (a, b) VALUES (uuid(), uuid())')

        # Later, we'll also make assertions about the behavior of non-CDC tables, so
        # we create one here.
        non_cdc_table_name = 'non_cdc_tab'
        session.execute('CREATE TABLE ' + ks_name + '.' + non_cdc_table_name + ' '
                        '(a uuid PRIMARY KEY, b uuid)')
        # We'll also make assertions about the behavior of CDC tables when
        # other CDC tables have already filled the designated space for CDC
        # commitlogs, so we create the second table here.
        emtpy_cdc_table_name = 'empty_cdc_tab'
        session.execute('CREATE TABLE ' + ks_name + '.' + emtpy_cdc_table_name + ' '
                        '(a uuid PRIMARY KEY, b uuid) '
                        'WITH CDC = true')

        # Here, we insert values into the first CDC table until we get a
        # WriteFailure. This should happen when the CDC commitlogs take up 1MB
        # or more.
        start, rows_loaded, error_found = time.time(), 0, False
        last_time_logged = start
        debug('beginning data insert to fill CDC commitlogs')
        while not error_found:
            # We want to fail if inserting data takes too long. Locally this
            # takes about a minute and a half, so let's be generous.
            self.assertLessEqual(
                (time.time() - start), 600,
                "It's taken more than 10 minutes to reach a WriteFailure trying "
                'to overrun the space designated for CDC commitlogs. This could '
                "be because data isn't being written quickly enough in this "
                'environment, or because C* is failing to reject writes when '
                'it should.'
            )

            # If we haven't logged from here in the last 5s, do so.
            if time.time() - last_time_logged > 5:
                debug('  data load step has lasted {s:.2f}s, '
                      'loaded {r} rows'.format(s=(time.time() - start), r=rows_loaded))
                last_time_logged = time.time()

            batch_results = list(execute_concurrent(
                session,
                # Insert values 100000 at a time...
                ((insert_stmt, ()) for _ in range(100000)),
                # with up to 500 connections.
                concurrency=500,
                # Don't propogate errors to the main thread. We expect at least
                # one WriteFailure, so we handle it below as part of the
                # results recieved from this method.
                raise_on_first_error=False
            ))

            # Here, we track the number of inserted values by getting the
            # number of successfully completed statements...
            rows_loaded += len((br for br in batch_results if br[0]))
            # then, we make sure that the only failures are the expected
            # WriteFailures.
            self.assertEqual((), (result for (success, result) in batch_results
                                  if not success and not isinstance(result, WriteFailure)))
            # Finally, if we find a WriteFailure, that means we've inserted all
            # the CDC data we can and so we flip error_found to exit the loop.
            if any(type(result) == WriteFailure for (_, result) in batch_results):
                debug("write failed (presumably because we've overrun "
                      'designated CDC commitlog space) after '
                      'loading {r} rows in {s:.2f}s'.format(
                          r=rows_loaded,
                          s=time.time() - start))
                error_found = True

        commitlog_dir = os.path.join(node.get_path(), 'commitlogs')
        commitlogs_size = size_of_files_in_dir(commitlog_dir)
        debug('Commitlog dir ({d}) is {b}B'.format(d=commitlog_dir, b=commitlogs_size))
        self.assertGreaterEqual(commitlogs_size, 1024 ** 2)

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
