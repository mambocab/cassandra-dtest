import codecs
import csv
import locale
import os
import random
import sys
from tempfile import NamedTemporaryFile
from uuid import uuid4

from cassandra.concurrent import execute_concurrent_with_args

from dtest import debug, Tester
from tools import rows_to_list


class _EmptyStringGetter(object):
    def __getitem__(self, *args):
        return ''


class CqlshCopyTest(Tester):

    def result_to_csv_rows(self, result, cassandra_dir=None):
        '''
        Given an object returned from a CQL query, returns a string formatted by
        the cqlsh formatting utilities.
        '''
        # unfortunately, it's not easy to grab the formatting function, so we
        # have to manually add its definition to the import path. Mega-gross.
        saved_path = list(sys.path)
        formatted = sigil = object()
        if cassandra_dir is None:
            cassandra_dir = self.cluster.nodelist()[0].get_install_dir()

        try:
            sys.path = sys.path + [os.path.join(cassandra_dir, 'pylib')]
            from cqlshlib.formatting import format_value
            encoding_name = codecs.lookup(locale.getpreferredencoding()).name
            def fmt(val):
                return format_value(type(val), val,
                                      encoding=encoding_name,
                                      date_time_format=None,
                                      float_precision=None,
                                      colormap=_EmptyStringGetter(),
                                      nullval=None).strval
            formatted = [[fmt(v) for v in row] for row in result]
        finally:
            sys.path = saved_path

        # if the try block failed, return None
        return None if formatted is sigil else formatted

    def test_copy_to(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node1, = self.cluster.nodelist()

        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )""")

        insert_statement = session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)]
        execute_concurrent_with_args(session, insert_statement, args)

        results = list(session.execute("SELECT * FROM testcopyto"))

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        node1.run_cqlsh(cmds="COPY ks.testcopyto TO '{name}'".format(name=tempfile.name))

        self.assertSequenceEqual(list(csv_rows(tempfile.name)),
                                 list(self.result_to_csv_rows(results)))

        # import the CSV file with COPY FROM
        session.execute("TRUNCATE ks.testcopyto")
        node1.run_cqlsh(cmds="COPY ks.testcopyto FROM '{name}'".format(name=tempfile.name))
        new_results = list(session.execute("SELECT * FROM testcopyto"))
        self.assertEqual(results, new_results)

    def test_list_data(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = self.cluster.nodelist()
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute("""
            CREATE TABLE testlist (
                a int PRIMARY KEY,
                b list<uuid>
            )""")

        insert_statement = session.prepare("INSERT INTO testlist (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4)) for i in range(1000)]
        execute_concurrent_with_args(session, insert_statement, args)

        results = list(session.execute("SELECT * FROM testlist"))

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        node.run_cqlsh(cmds="COPY ks.testlist TO '{name}'".format(name=tempfile.name))

        # csv_and_result_rows = zip(csv_rows(tempfile.name),
        #                           self.result_to_csv_rows(results))

        # for csv_row, result_row in csv_and_result_rows:
        #     self.assertEqual(csv_row, result_row)
        self.assertSequenceEqual(list(csv_rows(tempfile.name)),
                                 list(self.result_to_csv_rows(results)))


    def test_tuple_data(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [node] = self.cluster.nodelist()
        session = self.patient_cql_connection(node)
        self.create_ks(session, 'ks', 1)
        session.execute("""
            create table testlist (
                a int primary key,
                b tuple<uuid, uuid, uuid>
            )""")

        insert_statement = session.prepare("insert into testtuple (a, b) values (?, ?)")
        args = [(i, random_list(gen=uuid4, n=3)) for i in range(1000)]
        execute_concurrent_with_args(session, insert_statement, args)

        results = list(session.execute("select * from testtuple"))

        tempfile = namedtemporaryfile()
        debug('exporting to csv file: {name}'.format(name=tempfile.name))
        node.run_cqlsh(cmds="copy ks.testtuple to '{name}'".format(name=tempfile.name))

        self.assertsequenceequal(list(csv_rows(tempfile.name)),
                                 list(self.result_to_csv_rows(results)))


def csv_rows(filename):
    '''
    Given a filename, opens a csv file and yeilds it line by line.
    '''
    with open(filename, 'r') as csvfile:
        for row in csv.reader(csvfile):
            yield row


def random_list(gen=None, n=None):
    if gen is None:
        def gen():
            return random.randint(-1000, 1000)
    if n is None:
        def length():
            return random.randint(1, 5)
    else:
        def length():
            return n

    return [gen() for _ in range(length())]
