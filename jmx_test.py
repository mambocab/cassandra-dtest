import re
import sys
import unittest
import time

from dtest import Tester, debug
from jmxutils import JolokiaAgent, make_mbean, remove_perf_disable_shared_mem
from tools import since


class TestJMX(Tester):

    @since('2.1')
    @unittest.skipIf(sys.platform == "win32", 'Skip long tests on Windows')
    def cfhistograms_test(self):
        """
        Test cfhistograms on large and small datasets
        @jira_ticket CASSANDRA-8028
        """

        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        # issue large stress write to load data into cluster
        node1.stress(['write', 'n=15M', '-schema', 'replication(factor=3)', '-rate', 'threads=50'])
        node1.flush()

        # TODO the keyspace and table name are capitalized in 2.0
        histogram = node1.nodetool("cfhistograms keyspace1 standard1", capture_output=True)
        error_msg = "Unable to compute when histogram overflowed"
        debug(histogram)
        self.assertFalse(error_msg in histogram)
        self.assertTrue("NaN" not in histogram)

        session = self.patient_cql_connection(node1)

        session.execute("CREATE KEYSPACE test WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}")
        session.execute("CREATE TABLE test.tab(key int primary key, val int);")

        finalhistogram = node1.nodetool("cfhistograms test tab", capture_output=True)
        debug(finalhistogram)

        error_msg = "Unable to compute when histogram overflowed"
        self.assertFalse(error_msg in finalhistogram)
        correct_error_msg = "No SSTables exists, unable to calculate 'Partition Size' and 'Cell Count' percentiles"
        self.assertTrue(correct_error_msg in finalhistogram[1])

    @since('2.1')
    def netstats_test(self):
        """
        Check functioning of nodetool netstats, especially with restarts.
        @jira_ticket CASSANDRA-8122, CASSANDRA-6577
        """

        cluster = self.cluster
        cluster.populate(3).start(wait_for_binary_proto=True)
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=5M', '-schema', 'replication(factor=3)'])
        node1.flush()
        node1.stop(gently=False)
        try:
            node1.nodetool("netstats")
        except Exception as e:
            if "ConcurrentModificationException" in str(e):
                self.fail("Netstats failed due to CASSANDRA-6577")
            else:
                debug(str(e))

        node1.start(wait_for_binary_proto=True)

        try:
            node1.nodetool("netstats")
        except Exception as e:
            if 'java.lang.reflect.UndeclaredThrowableException' in str(e):
                debug(str(e))
                self.fail("Netstats failed with UndeclaredThrowableException (CASSANDRA-8122)")
            else:
                self.fail(str(e))

    @since('2.1')
    def table_metric_mbeans_test(self):
        """
        Test some basic table metric mbeans with simple writes.
        """
        cluster = self.cluster
        cluster.populate(3)
        node1, node2, node3 = cluster.nodelist()
        remove_perf_disable_shared_mem(node1)
        cluster.start(wait_for_binary_proto=True)

        version = cluster.version()
        if version < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=10000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

        typeName = "ColumnFamily" if version <= '2.2.X' else 'Table'
        debug('Version {} typeName {}'.format(version, typeName))

        # TODO the keyspace and table name are capitalized in 2.0
        memtable_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='AllMemtablesHeapSize')
        disk_size = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='LiveDiskSpaceUsed')
        sstable_count = make_mbean('metrics', type=typeName, keyspace='keyspace1', scope='standard1', name='LiveSSTableCount')

        with JolokiaAgent(node1) as jmx:
            mem_size = jmx.read_attribute(memtable_size, "Value")
            self.assertGreater(int(mem_size), 10000)

            on_disk_size = jmx.read_attribute(disk_size, "Count")
            self.assertEquals(int(on_disk_size), 0)

            node1.flush()

            on_disk_size = jmx.read_attribute(disk_size, "Count")
            self.assertGreater(int(on_disk_size), 10000)

            sstables = jmx.read_attribute(sstable_count, "Value")
            self.assertGreaterEqual(int(sstables), 1)

    def test_compactionstats(self):
        """
        @jira_ticket CASSANDAR-10504
        @jira_ticket CASSANDRA-10427

        Test that jmx MBean used by nodetool compactionstats
        properly updates the progress of a compaction
        """

        cluster = self.cluster
        cluster.populate(1)
        node = cluster.nodelist()[0]
        cluster.set_configuration_options({'concurrent_compactors': 1, 'memtable_cleanup_threshold': 0.01})
        remove_perf_disable_shared_mem(node)
        cluster.start(wait_for_binary_proto=True)

        # Run a quick stress command to create the keyspace and table
        node.stress(['write', 'n=1'])
        # Disable compaction on the table
        node.nodetool('disableautocompaction keyspace1 standard1')

        node.stress(['write', 'n=750K'])
        # Run a major compaction. This will be the compaction whose
        # progress we track.
        node.nodetool('compact', capture_output=False, wait=False)
        # We need to sleep here to give compaction time to start
        # Why not do something smarter? Because if the bug regresses,
        # we can't rely on jmx to tell us that compaction started.
        time.sleep(5)

        compaction_manager = make_mbean('db', type='CompactionManager')

        with JolokiaAgent(node) as jmx:
            progress_string = jmx.read_attribute(compaction_manager, 'CompactionSummary')[0]

            # Pause in between reads
            # to allow compaction to move forward
            time.sleep(2)

            updated_progress_string = jmx.read_attribute(compaction_manager, 'CompactionSummary')[0]

            progress = int(re.search('standard1, (\d+)\/', progress_string).groups()[0])
            updated_progress = int(re.search('standard1, (\d+)\/', updated_progress_string).groups()[0])

            debug(progress_string)
            debug(updated_progress_string)

            # We want to make sure that the progress is increasing,
            # and that values other than zero are displayed.
            self.assertGreater(updated_progress, progress)
            self.assertGreater(progress, 0)
            self.assertGreater(updated_progress, 0)

            # Block until the major compaction is complete
            # Otherwise nodetool will throw an exception
            # Give a timeout, in case compaction is broken
            # and never ends.
            start = time.time()
            max_query_timeout = 600
            debug("Waiting for compaction to finish:")
            while (len(jmx.read_attribute(compaction_manager, 'CompactionSummary')) > 0) and (time.time() - start < max_query_timeout):
                debug(jmx.read_attribute(compaction_manager, 'CompactionSummary'))
                time.sleep(2)
