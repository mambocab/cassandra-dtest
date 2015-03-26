from dtest import Tester
# from tools import require
from tools import since, rows_to_list
# from assertions import assert_all
from unittest import skip

import time


# @require('6237')
@since('3.0')
class TestRangeDeletion(Tester):
    '''
    Tests the range deletion feature described in
    https://issues.apache.org/jira/browse/CASSANDRA-6237.
    '''

    def in_filter_test(self):
        """Test for IN filter"""

        self.cluster.populate(1).start()
        [node1] = self.cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('''
            CREATE TABLE test (
                key int PRIMARY KEY
            );''')

        simple_insert = cursor.prepare("INSERT INTO test (key) VALUES (?)")
        for k in range(5):
            cursor.execute(simple_insert, [k])

        cursor.execute("DELETE FROM test WHERE key IN (3, 1);")

        result = rows_to_list(cursor.execute('SELECT key FROM test;'))
        self.assertEqual(result, [[0], [2], [4]])

    def multiple_filter_test(self):
        """Test for >= filter"""

        self.cluster.populate(1).start()
        [node1] = self.cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('''
            CREATE TABLE test (
                partition_key int,
                i int,
                PRIMARY KEY (partition_key, i)
            );''')

        simple_insert = cursor.prepare(
            'INSERT INTO test (partition_key, i) VALUES (?, ?)')
        for j in (1, 2, 3):
            for k in range(5):
                cursor.execute(simple_insert, [j, k])

        cursor.execute('DELETE FROM test WHERE partition_key = 3 AND i > 2;')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[1, 0], [1, 1], [1, 2], [1, 3], [1, 4],
                                  [2, 0], [2, 1], [2, 2], [2, 3], [2, 4],
                                  [3, 0], [3, 1], [3, 2]])

    def equality_filtered_batch_test(self):
        self.cluster.populate(1).start()
        time.sleep(5)
        [node1] = self.cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('''
            CREATE TABLE test (
                partition_key int,
                i int,
                PRIMARY KEY (partition_key, i)
            );
                       ''')

        simple_insert = cursor.prepare(
            'INSERT INTO test (partition_key, i) VALUES (?, ?)')
        for j in (1, 2, 3):
            for k in range(5):
                cursor.execute(simple_insert, [j, k])

        cursor.execute('''
            BEGIN BATCH
            DELETE FROM test WHERE partition_key = 1
            DELETE FROM test WHERE partition_key = 2 AND i IN (0, 4)
            APPLY BATCH;
                       ''')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[2, 1], [2, 2], [2, 3],
                                  [3, 0], [3, 1], [3, 2], [3, 3], [3, 4]])

    @skip('not yet implemented')
    def inequality_filtered_batch_test(self):
        self.cluster.populate(1).start()
        time.sleep(5)
        [node1] = self.cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('''
            CREATE TABLE test (
                partition_key int,
                i int,
                PRIMARY KEY (partition_key, i)
            );
                       ''')

        simple_insert = cursor.prepare(
            'INSERT INTO test (partition_key, i) VALUES (?, ?)')
        for j in (1, 2, 3):
            for k in range(5):
                cursor.execute(simple_insert, [j, k])

        cursor.execute('''
            BEGIN BATCH
            DELETE FROM test WHERE partition_key = 2 AND i >= 3
            APPLY BATCH;
                       ''')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[2, 1], [2, 2], [2, 3],
                                  [3, 0], [3, 1], [3, 2], [3, 3], [3, 4]])

    @skip("check with implementer: unsupported?")
    def delete_manually_indexed_key_inequality_test(self):
        self.cluster.populate(1).start()
        time.sleep(5)
        [node1] = self.cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('''
            CREATE TABLE test (
                key int PRIMARY KEY,
                i int
            );
                       ''')

        simple_insert = cursor.prepare(
            'INSERT INTO test (key, i) VALUES (?, ?)')
        for j in (1, 2, 3):
            for k in range(5):
                cursor.execute(simple_insert, [j, k])

        cursor.execute('CREATE INDEX ON test (i)')

        cursor.execute('''
            DELETE FROM test WHERE key = 2 AND i < 3
                       ''')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[1, 0], [1, 1], [1, 2], [1, 3], [1, 4],
                                  [2, 3], [2, 4],
                                  [3, 0], [3, 1], [3, 2], [3, 3], [3, 4]])

    def delete_manually_indexed_key_equality_test(self):
        self.cluster.populate(1).start()
        time.sleep(5)
        [node1] = self.cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        self.create_ks(cursor, 'ks', 1)
        cursor.execute('''
            CREATE TABLE test (
                key int PRIMARY KEY,
                i int
            );
                       ''')

        simple_insert = cursor.prepare(
            'INSERT INTO test (key, i) VALUES (?, ?)')
        for j in (1, 2, 3):
            for k in range(5):
                cursor.execute(simple_insert, [j, k])

        cursor.execute('CREATE INDEX ON test (i)')

        cursor.execute('''
            DELETE FROM test WHERE key = 2 AND i = 3
                       ''')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[1, 0], [1, 1], [1, 2], [1, 3], [1, 4],
                                  [2, 0], [2, 1], [2, 2], [2, 4],
                                  [3, 0], [3, 1], [3, 2], [3, 3], [3, 4]])
