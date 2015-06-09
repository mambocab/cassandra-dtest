from dtest import Tester

import ast
import time
from jmxutils import make_mbean, JolokiaAgent, remove_perf_disable_shared_mem
from tools import since
import os
from ccmlib import common

class TestConfiguration(Tester):

    def compression_chunk_length_test(self):
        """ Verify the setting of compression chunk_length [#3558]"""
        cluster = self.cluster

        cluster.populate(1).start()
        node = cluster.nodelist()[0]
        cursor = self.patient_cql_connection(node)
        self.create_ks(cursor, 'ks', 1)

        create_table_query = "CREATE TABLE test_table (row varchar, name varchar, value int, PRIMARY KEY (row, name));"
        alter_chunk_len_query = "ALTER TABLE test_table WITH compression = {{'sstable_compression' : 'SnappyCompressor', 'chunk_length_kb' : {chunk_length}}};"

        cursor.execute( create_table_query)

        cursor.execute( alter_chunk_len_query.format(chunk_length=32) )
        self._check_chunk_length( cursor, 32 )

        cursor.execute( alter_chunk_len_query.format(chunk_length=64) )
        self._check_chunk_length( cursor, 64 )

    def change_durable_writes_test(self):
        """
        @jira_ticket 9560
        """
        # writes should block on commitlog fsync
        self.cluster.populate(1)
        node = self.cluster.nodelist()[0]
        self.cluster.set_configuration_options(values={'commitlog_segment_size_in_mb': 1}, batch_commitlog=True)

        # disable JVM option so we can use Jolokia
        # this has to happen after .set_configuration_options because of implmentation details
        remove_perf_disable_shared_mem(node)
        self.cluster.start(wait_for_binary_proto=True)
        cursor = self.patient_cql_connection(node)

        commitlog_size_mbean = make_mbean('metrics', type='CommitLog', name='TotalCommitLogSize')

        def commit_log_size():
            with JolokiaAgent(node) as jmx:
                return jmx.read_attribute(commitlog_size_mbean, 'Value')

        init_size = commit_log_size()
        cursor.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1} "
                       "AND DURABLE_WRITES = true")
        cursor.execute('CREATE TABLE ks.tab (key int PRIMARY KEY, a int)')
        stress_write(node)

        debug('with durable writes: size diff = ' + str(commit_log_size() - init_size))
        # self.assertEqual(init_size, commit_log_size())

        init_size = commit_log_size()
        cursor.execute("CREATE KEYSPACE ks2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1} "
                       "AND DURABLE_WRITES = false")
        cursor.execute('CREATE TABLE ks.tab (key int PRIMARY KEY, a int)')
        stress_write(node)
        debug('without durable writes: size diff = ' + str(commit_log_size() - init_size))

        # self.assertEqual(init_size, commit_log_size())

        # cursor.execute('ALTER KEYSPACE ks WITH DURABLE_WRITES = true')

        # self.assertEqual(init_size, commit_log_size())

        # cursor.execute('INSERT INTO ks.tab (key, a) VALUES (2, 5)')

        # self.assertLess(init_size, commit_log_size())

    def _check_chunk_length(self, cursor, value):
        describe_table_query = "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name='ks' AND columnfamily_name='test_table';"
        rows = cursor.execute( describe_table_query )
        results = rows[0]
        #Now extract the param list
        params = ''
        for result in results:
            if 'sstable_compression' in str(result):
                params = result

        assert params is not '', "Looking for a row with the string 'sstable_compression' in system.schema_columnfamilies, but could not find it."

        params = ast.literal_eval( params )
        chunk_length = int( params['chunk_length_kb'] )

        assert chunk_length == value, "Expected chunk_length: %s.  We got: %s" % (value, chunk_length)


def stress_write(node):
    if node.get_cassandra_version() < '2.1':
        node.stress(['--num-keys=100000'])
    else:
        node.stress(['write', 'n=100000'])
