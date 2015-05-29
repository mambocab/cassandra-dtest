from uuid import uuid4
from dtest import Tester
from tools import require, debug
from random import random, randint
from cassandra.concurrent import execute_concurrent_with_args
from jmxutils import make_mbean, JolokiaAgent, remove_perf_disable_shared_mem


# @require(8099)
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

    @note We don't know how the new row format affects schema changes.
    (Honestly, I don't know what schema changes look like under the hood now.)
    """

    def setUp(self):
        Tester.setUp(self)
        self.cluster.populate(1)
        self.node1 = self.cluster.nodelist()[0]
        remove_perf_disable_shared_mem(self.node1)
        self.cluster.start(wait_for_binary_proto=True)

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
        # old_format_version = '2.2.0'

        ks, table = 'ks1', 'tab1'

        self.write_graphlike_data(ks, table, sparse=True)
        session = self.patient_exclusive_cql_connection(self.node1)

        disk_used = sstables_size(self.node1, ks, table)
        debug(disk_used)

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

    def upgrade_to_version(self, tag, nodes=None):
        """
        copied from upgrade_supercolumns_test
        """
        debug('Upgrading to ' + tag)
        if nodes is None:
            nodes = self.cluster.nodelist()

        for node in nodes:
            debug('Shutting down node: ' + node.name)
            node.drain()
            node.watch_log_for("DRAINED")
            node.stop(wait_other_notice=False)

        # Update Cassandra Directory
        for node in nodes:
            node.set_install_dir(version=tag)
            debug("Set new cassandra dir for %s: %s" % (node.name, node.get_install_dir()))
        self.cluster.set_install_dir(version=tag)

        # Restart nodes on new version
        for node in nodes:
            debug('Starting %s on new version (%s)' % (node.name, tag))
            # Setup log4j / logback again (necessary moving from 2.0 -> 2.1):
            node.set_log_level("INFO")
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            # node.nodetool('upgradesstables -a')  # not necessary; we're not reading the old data again

    def write_graphlike_data(self, ks_name, table_name, sparse):
        """
        Ideally, we can do this with cassandra-stress. Until then, we'll do it manually.
        """
        session = self.patient_exclusive_cql_connection(self.cluster.nodelist()[0])

        num_columns = 1000
        column_names = list(islice(unique_names(), num_columns))

        self.create_ks(session, ks_name, 1)
        self.create_cf(session, table_name, key_type='uuid',
                       columns={k: 'int' for k in column_names})

        null_prob = .3 if sparse else .7

        data = [[uuid4()] + [random_int(null_prob) for x in range(num_columns)]
                for y in range(10000)]
        insert_cql = 'INSERT INTO ' + table_name
        insert_cql += ' (' + ', '.join(['key'] + column_names) + ' ) '
        insert_cql += ' VALUES '
        insert_cql += ' (' + ', '.join(['?' for x in range(num_columns + 1)]) + ')'

        execute_concurrent_with_args(session, session.prepare(insert_cql), data)


from itertools import product, tee, islice
from string import ascii_lowercase as letters


def unique_names(min_length=5):
    generation = ()
    while True:
        if not generation:
            generation = yielder = letters
        else:
            generation, yielder = tee((a + b for a, b in product(generation, letters)))
        for g in yielder:
            if len(g) >= min_length:
                yield g


def random_int(null_prob):
    if null_prob < random():
        return None
    else:
        return randint(-2 ** 30, 2 ** 30)


def sstables_size(node, keyspace, table):
    return columnfamily_metric(node, keyspace, table, 'LiveDiskSpaceUsed')


def columnfamily_metric(node, keyspace, table, name):
    with JolokiaAgent(node) as jmx:
        mbean = make_mbean('metrics', type='ColumnFamily',
                           name=name, keyspace=keyspace, scope=table)
        value = jmx.read_attribute(mbean, 'Count')
    return value
