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
        saved_path = list(sys.path)
        cassandra_dir = self.cluster.nodelist()[0].get_install_dir()

        try:
            sys.path = sys.path + [os.path.join(cassandra_dir, 'pylib')]
            import cqlshlib
            yield cqlshlib
        finally:
            sys.path = saved_path

    def result_to_csv_rows(self, result):
        '''
        Given an object returned from a CQL query, returns a string formatted by
        the cqlsh formatting utilities.
        '''
        # unfortunately, it's not easy to grab the formatting function, so we
        # have to manually add its definition to the import path. Mega-gross.
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

    def test_copy_to(self):
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

        self.assertSequenceEqual(list(csv_rows(tempfile.name)),
                                 list(self.result_to_csv_rows(results)))

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

        # csv_and_result_rows = zip(csv_rows(tempfile.name),
        #                           self.result_to_csv_rows(results))

        # for csv_row, result_row in csv_and_result_rows:
        #     self.assertEqual(csv_row, result_row)
        self.assertSequenceEqual(list(csv_rows(tempfile.name)),
                                 list(self.result_to_csv_rows(results)))

    def test_tuple_data(self):
        self.prepare()
        self.session.execute("""
            create table testlist (
                a int primary key,
                b tuple<uuid, uuid, uuid>
            )""")

        insert_statement = self.session.prepare("insert into testtuple (a, b) values (?, ?)")
        args = [(i, random_list(gen=uuid4, n=3)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = list(self.session.execute("select * from testtuple"))

        tempfile = NamedTemporaryFile()
        debug('exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="copy ks.testtuple to '{name}'".format(name=tempfile.name))

        self.assertsequenceequal(list(csv_rows(tempfile.name)),
                                 list(self.result_to_csv_rows(results)))
