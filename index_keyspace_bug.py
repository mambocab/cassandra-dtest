from cassandra.protocol import SyntaxException

from dtest import Tester


class IndexKeyspaceTest(Tester):

    def setUp(self):
        super(IndexKeyspaceTest, self).setUp()
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [self.node1] = self.cluster.nodelist()
        self.cursor = self.patient_cql_connection(self.node1)
        self.create_ks(self.cursor, 'ks', 1)
        self.create_cf(self.cursor, 'test', columns={'i': 'int'})

    def delete_index_test(self):
        try:
            self.cursor.execute('DROP INDEX ks.index_to_drop;')
        except Exception as e:
            # other errors are fine -- e.g., they keyspace doesn't exist
            if isinstance(e, SyntaxException):
                raise e
