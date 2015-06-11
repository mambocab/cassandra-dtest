from __future__ import division

import time
from itertools import islice, product, tee
from random import randint, random
from string import ascii_lowercase as letters
from uuid import uuid4

from dtest import Tester
from jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem
from tools import debug


class TestNewRowFormat(Tester):
    """
    @jira_ticket 8099

    Tests that the new row format's desirable properties hold.

    The new row format (at least as documented
    [here](https://github.com/pcmanus/cassandra/blob/8099/guide_8099.md))
    should always be smaller than the old, there are two formats, "sparse" and "dense".
    Which format to use is chosen based on statistics over the rows being
    written to an SSTable.

    To simulate graph datasets, the data used for these tests should have
    (10s of? 100s of?) thousands of columns, but with fewer than 50 rows
    populated per column.

    @note We don't know yet if cassandra-stress is a useful data generator
    for these tests. It seems that some features would have
    to be added -- in particular, some control over the likelihood that a
    given column will be written, or the sparseness of rows.

    The change to the row format could break schema changes in all kinds of
    ways. However, we don't know of any particular failures to look out for.
    Since we can't do much better than the existing test suite, schema change
    correctness tests will be handled by the fuzz-testing harness.
    """

    def setUp(self, version=None, install_dir=None):
        Tester.setUp(self)
        if version is not None or install_dir is not None:
            self.cluster.set_install_dir(version=version, install_dir=install_dir)
        self.cluster.populate(1)
        self.node1 = self.cluster.nodelist()[0]
        remove_perf_disable_shared_mem(self.node1)
        self.cluster.start(wait_for_binary_proto=True)

    def set_new_cluster(self, version=None, install_dir=None):
        self.tearDown()
        self.setUp(version=version, install_dir=install_dir)

    def write_graphlike_data(self, ks_name, table_name, sparse, n=10000, num_columns=1000):
        """
        Writes 10000 values to ks_name.table_name. If sparse, 70% of the
        values written will be null; if not sparse, 30% of the values will be
        null.

        Ideally, in the future, we can do this with cassandra-stress, which
        will give us easy flexibility for datatypes, etc.
        """
        session = self.patient_exclusive_cql_connection(self.cluster.nodelist()[0])

        column_names = list(islice(unique_names(), num_columns))

        self.create_ks(session, ks_name, 1)
        self.create_cf(session, table_name, key_type='uuid',
                       columns={k: 'int' for k in column_names})

        null_prob = .3 if sparse else .7

        data = ([uuid4()] + [random_int_or_null(null_prob) for x in range(num_columns)]
                for y in range(n))

        insert_cql = 'INSERT INTO ' + table_name
        insert_cql += ' (' + ', '.join(['key'] + column_names) + ' ) '
        insert_cql += ' VALUES '
        insert_cql += ' (' + ', '.join(['?' for x in range(num_columns + 1)]) + ')'
        debug('preparing...')
        prepared = session.prepare(insert_cql)

        for i, d in enumerate(data):
            session.execute(prepared, d)

        self.cluster.flush()


class SSTableSizeTest(TestNewRowFormat):

    def dense_sstables_smaller_test(self):
        """
        Test that SSTables are smaller in the 3.0 representation than in the old by:

        - on both a 3.0 and 2.2 cluster:
            - write data to cluster (same data)
            - flush all data in the cluster.

        The dataset should be one that will be represented in the "dense"
        variant of the new format.

        The total on-disk size of the data on the 3.0 cluster should be smaller.
        """

        def disk_used_for_install(ks='ks', table='tab', install_dir=None, version=None):
            if install_dir is not None or version is not None:
                self.set_new_cluster(install_dir=install_dir, version=version)
            self.write_graphlike_data(ks, table, sparse=False)
            disk_used = sstables_size(self.node1, ks, table)
            debug('disk used by {}: {}'.format(self.cluster.version(), disk_used))
            return disk_used

        new_size = disk_used_for_install()
        old_size = disk_used_for_install(version='git:cassandra-2.2')

        debug('new/old = {}'.format(new_size / old_size))
        self.assertGreater(old_size, new_size)

    def sparse_sstables_smaller_test(self):
        """
        Test that SSTables written in the 3.0 representation with sparse rows
        are smaller than in the old by:

        - on both a 3.0 and 2.2 cluster:
            - write data to cluster (same data)
            - flush all data in the cluster.

        The dataset should be one that will be represented in the "sparse"
        variant of the new format.

        The total on-disk size of the data on the 3.0 cluster should be smaller.
        """
        def disk_used_for_install(ks='ks', table='tab', install_dir=None, version=None):
            if install_dir is not None or version is not None:
                self.set_new_cluster(install_dir=install_dir, version=version)
            self.write_graphlike_data(ks, table, sparse=True)
            disk_used = sstables_size(self.node1, ks, table)
            debug('disk used by {}: {}'.format(self.cluster.version(), disk_used))
            return disk_used

        new_size = disk_used_for_install()
        old_size = disk_used_for_install(version='git:cassandra-2.2')

        debug('new/old = {}'.format(new_size / old_size))
        self.assertGreater(old_size, new_size)


class CompactionSpeedTest(TestNewRowFormat):
    def compaction_speed_test(self):
        """
        @test_assumptions spinning storage media

        Quantify the speed differences in compaction in the 3.0 representation by:

        - on both a 3.0 and 2.2 cluster:
            - write data to cluster (same data)
            - flush all data in the cluster,
            - run compaction.

        The newer representation requires more seeks and will likely be slower
        on spinning disks (or would be slower, if nothing else changed between
        2.2 and 3.0).

        This should probably be measured with:

        - datasets represented in the sparse and dense variants of the new format
        - all compaction strategies.

        This test should be run on spinning storage media.
        """
        def compaction_time(ks='ks', table='tab', install_dir=None, version=None):
            if install_dir is not None or version is not None:
                self.set_new_cluster(install_dir=install_dir, version=version)
            self.write_graphlike_data(ks, table, sparse=False)

            start = time.time()
            self.cluster.compact()
            result = time.time() - start
            debug(result)
            return result

        new_time = compaction_time()
        old_time = compaction_time(version='git:cassandra-2.2')

        debug('new/old = {}'.format(new_time / old_time))
        self.assertGreater(new_time, old_time)


class SchemaChangeTest(TestNewRowFormat):
    def schema_change_speed_test(self):
        def schema_change_time(ks, table, install_dir=None, version=None):
            if install_dir is not None or version is not None:
                self.set_new_cluster(install_dir=install_dir, version=version)
            self.write_graphlike_data(ks, table, sparse=False)

            session = self.patient_exclusive_cql_connection(self.cluster.nodelist()[0])

            start = time.time()

            session.execute('ALTER TABLE {}.{} DROP aaaaa'.format(ks, table))

            result = time.time() - start
            debug(result)
            return result

        new_time = schema_change_time('ks1', 'tab1')
        old_time = schema_change_time('ks2', 'tab2',
                                      version='git:cassandra-2.2')

        debug('new/old = {}'.format(new_time / old_time))
        self.assertGreater(new_time, old_time)


def unique_names(min_length=5):
    """
    Infinitely yields a sequence of strings of the form

    a, b, c... aa, ab, ac... ba, bb, bc... aaa, aab, aac...

    starting with the first string of length `min_length`.
    """
    generation = ()
    while True:
        if not generation:
            generation = yielder = letters
        else:
            generation, yielder = tee((a + b for a, b in product(generation, letters)))
        for g in yielder:
            if len(g) >= min_length:
                yield g


def random_int_or_null(null_prob):
    if null_prob < random():
        return None
    else:
        return randint(-(2 ** 30), 2 ** 30)


def sstables_size(node, keyspace, table):
    return columnfamily_count_metric(node, keyspace, table, 'LiveDiskSpaceUsed')


def columnfamily_count_metric(node, keyspace, table, name):
    with JolokiaAgent(node) as jmx:
        mbean = make_mbean('metrics', type='ColumnFamily',
                           name=name, keyspace=keyspace, scope=table)
        value = jmx.read_attribute(mbean, 'Count')
    return value
