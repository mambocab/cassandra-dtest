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

    @skip('not yet implemented')
    def multiple_range_test(self):
        """Test for deletion with >= filter"""
        cursor = self._set_up_and_get_cursor()

        cursor.execute('DELETE FROM test WHERE key = 3 AND i > 2;')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[1, 0], [1, 1], [1, 2], [1, 3], [1, 4],
                                  [2, 0], [2, 1], [2, 2], [2, 3], [2, 4],
                                  [3, 0], [3, 1], [3, 2]])

    @skip('not yet implemented')
    def equality_range_batch_test(self):
        cursor = self._set_up_and_get_cursor()

        cursor.execute('''
            BEGIN BATCH
            DELETE FROM test WHERE key = 1
            DELETE FROM test WHERE key = 2 AND i = 0
            APPLY BATCH;
                       ''')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[2, 1], [2, 2], [2, 3], [2, 4],
                                  [3, 0], [3, 1], [3, 2], [3, 3], [3, 4]])

    @skip('not yet implemented')
    def inequality_range_batch_test(self):
        cursor = self._set_up_and_get_cursor()

        cursor.execute('''
            BEGIN BATCH
            DELETE FROM test WHERE key = 2 AND i >= 2
            APPLY BATCH;
                       ''')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[1, 0], [1, 1], [1, 2], [1, 3], [1, 4],
                                  [2, 1], [2, 2],
                                  [3, 0], [3, 1], [3, 2], [3, 3], [3, 4]])

    @skip('maybe out of scope. check with implementer.')
    def delete_manually_indexed_key_inequality_range_test(self):
        cursor = self._set_up_and_get_cursor()

        cursor.execute('CREATE INDEX ON test (i)')

        cursor.execute('''
            DELETE FROM test WHERE key = 2 AND i < 3
                       ''')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[1, 0], [1, 1], [1, 2], [1, 3], [1, 4],
                                  [2, 3], [2, 4],
                                  [3, 0], [3, 1], [3, 2], [3, 3], [3, 4]])

    @skip('not implemented')
    def delete_manually_indexed_key_equality_range_test(self):
        cursor = self._set_up_and_get_cursor()

        cursor.execute('CREATE INDEX ON test (i)')

        cursor.execute('''
            DELETE FROM test WHERE key = 2 AND i > 3
                       ''')

        result = rows_to_list(cursor.execute('SELECT * FROM test;'))
        self.assertEqual(result, [[1, 0], [1, 1], [1, 2], [1, 3], [1, 4],
                                  [2, 0], [2, 1], [2, 2], [2, 3],
                                  [3, 0], [3, 1], [3, 2], [3, 3], [3, 4]])

    @skip('not implemented')
    def delete_range_using_timestamp(self):
        cursor = self._set_up_and_get_cursor()

        cursor.execute('''
            DELETE FROM test WHERE key = 2 and i < 2
            USING TIMESTAMP 20000000000000000
                       ''')

        result = rows_to_list(cursor.execute('SELECT i FROM test WHERE key = 2;'))
        self.assertEqual(result, [[0], [1], [2]])

        cursor.execute('''
            INSERT INTO test (key, i) values (2, 10)
            USING TIMESTAMP 20000000000000001
                       ''')
        result = rows_to_list(cursor.execute('SELECT i FROM test WHERE key = 2;'))
        self.assertEqual(result, [[0], [1], [2], [10]])

    def _set_up_and_get_cursor(self):
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

        return cursor
