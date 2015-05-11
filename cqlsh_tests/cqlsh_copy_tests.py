import codecs
from contextlib import contextmanager
import locale
import os
import random
import sys
from tempfile import NamedTemporaryFile
from uuid import uuid4

from cassandra.concurrent import execute_concurrent_with_args

from dtest import debug, Tester
from tools import rows_to_list
from cqlsh_tools import csv_rows, random_list, DummyColorMap


DEFAULT_FLOAT_PRECISION = 5  # magic number copied from cqlsh script


class CqlshCopyTest(Tester):

    @contextmanager
    def _cqlshlib(self):
        '''
        Returns the cqlshlib module, as defined in self.cluster's first node.
        '''
        # This method accomplishes its goal by manually adding the library to
        # sys.path, returning the module, then restoring the old path once the
        # context manager exits. This isn't great for maintainability and should
        # be replaced if cqlshlib is made easier to interact with.
        saved_path = list(sys.path)
        cassandra_dir = self.cluster.nodelist()[0].get_install_dir()

        try:
            sys.path = sys.path + [os.path.join(cassandra_dir, 'pylib')]
            import cqlshlib
            yield cqlshlib
        finally:
            sys.path = saved_path

    def assertCsvResultEqual(self, csv_filename, results):
        self.assertSequenceEqual(list(csv_rows(csv_filename)),
                                 list(self.result_to_csv_rows(results)))

    def result_to_csv_rows(self, result):
        '''
        Given an object returned from a CQL query, returns a string formatted by
        the cqlsh formatting utilities.
        '''
        with self._cqlshlib() as cqlshlib:
            from cqlshlib.formatting import format_value
            encoding_name = codecs.lookup(locale.getpreferredencoding()).name

            def fmt(val):
                return format_value(type(val), val,
                                    encoding=encoding_name,
                                    date_time_format=None,
                                    float_precision=DEFAULT_FLOAT_PRECISION,
                                    colormap=DummyColorMap(),
                                    nullval=None).strval

        return [[fmt(v) for v in row] for row in result]

    def prepare(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        self.node1, = self.cluster.nodelist()
        self.session = self.patient_cql_connection(self.node1)
        self.create_ks(self.session, 'ks', 1)

    def test_round_trip(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )""")

        insert_statement = self.session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testcopyto"))

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testcopyto TO '{name}'".format(name=tempfile.name))

        self.assertCsvResultEqual(tempfile.name, results)

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        debug('Importing from csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testcopyto FROM '{name}'".format(name=tempfile.name))
        new_results = list(self.session.execute("SELECT * FROM testcopyto"))
        self.assertEqual(results, new_results)

    def test_list_data(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testlist (
                a int PRIMARY KEY,
                b list<uuid>
            )""")

        insert_statement = self.session.prepare("INSERT INTO testlist (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testlist"))

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testlist TO '{name}'".format(name=tempfile.name))

        self.assertCsvResultEqual(tempfile.name, results)

    def test_tuple_data(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testtuple (
                a int primary key,
                b tuple<uuid, uuid, uuid>
            )""")

        insert_statement = self.session.prepare("INSERT INTO testtuple (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4, n=3)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testtuple"))

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testtuple TO '{name}'".format(name=tempfile.name))

        self.assertCsvResultEqual(tempfile.name, results)

    def non_default_delimiter_template(self, delimiter):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testdelimiter (
                a int primary key
            )""")
        insert_statement = self.session.prepare("INSERT INTO testdelimiter (a) VALUES (?)")
        args = [(i,) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("SELECT * FROM testdelimiter"))

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testdelimiter TO '{name}'".format(name=tempfile.name)
        cmds += " WITH DELIMITER = '{d}'".format(d=delimiter)
        self.node1.run_cqlsh(cmds=cmds)

        self.assertCsvResultEqual(tempfile.name, results)

    def test_colon_delimiter(self):
        self.non_default_delimiter_template(':')

    def test_semicolon_delimiter(self):
        self.non_default_delimiter_template(';')

    def test_letter_delimiter(self):
        self.non_default_delimiter_template('a')

    def test_number_delimiter(self):
        self.non_default_delimiter_template('1')
