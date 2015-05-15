# coding: utf-8
import codecs
from contextlib import contextmanager
import csv
import datetime
from decimal import Decimal
import locale
import os
import random
import sys
from tempfile import NamedTemporaryFile
import unittest
from uuid import uuid1, uuid4

from cassandra.concurrent import execute_concurrent_with_args

from dtest import debug, Tester
from tools import rows_to_list, since
from cqlsh_tools import csv_rows, random_list, DummyColorMap, assert_csvs_items_equal


DEFAULT_FLOAT_PRECISION = 5  # magic number copied from cqlsh script


class _CqlshCopyBase(Tester):

    def prepare(self):
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        self.node1, = self.cluster.nodelist()
        self.session = self.patient_cql_connection(self.node1)
        self.create_ks(self.session, 'ks', 1)

    def all_datatypes_prepare(self):
        self.prepare()
        self.session.execute('''
            CREATE TABLE testdatatype (
                a ascii PRIMARY KEY,
                b bigint,
                c blob,
                d boolean,
                e decimal,
                f double,
                g float,
                h inet,
                i int,
                j text,
                k timestamp,
                l timeuuid,
                m uuid,
                n varchar,
                o varint
            )''')
        self.data = ('ascii',  # a ascii
                     2 ** 40,  # b bigint
                     0111,  # c blob
                     True,  # d boolean
                     Decimal(3.14),  # e decimal
                     2.444,  # f double
                     1.1,  # g float
                     '127.0.0.1',  # h inet
                     25,  # i int
                     'ヽ(´ー｀)ノ',  # j text
                     datetime.datetime(2005, 7, 14, 12, 30),  # k timestamp
                     uuid1(),  # l timeuuid
                     uuid4(),  # m uuid
                     'asdf',  # n varchar
                     2 ** 65  # o varint
                     )


# Reading from csv files to cqlsh-formatted strings would require extensive use
# of the deprecated cassandra-dbapi2 project, so we skip all but the simplest
# tests on pre-2.1 versions.
@since('2.1')
class CqlshCopyTest(_CqlshCopyBase):

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
        self.assertItemsEqual(list(csv_rows(csv_filename)),
                              list(self.result_to_csv_rows(results)))

    def result_to_csv_rows(self, result):
        '''
        Given an object returned from a CQL query, returns a string formatted by
        the cqlsh formatting utilities.
        '''
        # This has no real dependencies on Tester except that self._cqlshlib has
        # to grab self.cluster's install directory. This should be pulled out
        # into a bare function if cqlshlib is made easier to interact with.
        with self._cqlshlib() as cqlshlib:
            from cqlshlib.formatting import format_value
            try:
                # doesn't properly import on 2.1, but making
                from cqlshlib.formatting import DateTimeFormat
                date_time_format = DateTimeFormat()
            except ImportError:
                date_time_format = None
            encoding_name = codecs.lookup(locale.getpreferredencoding()).name

            def fmt(val):
                # different versions use time_format or date_time_format
                # but all versions reject spurious values, so we just use both
                # here
                return format_value(type(val), val,
                                    encoding=encoding_name,
                                    date_time_format=date_time_format,
                                    time_format=None,
                                    float_precision=DEFAULT_FLOAT_PRECISION,
                                    colormap=DummyColorMap(),
                                    nullval=None).strval

        return [[fmt(v) for v in row] for row in result]

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

    def test_letter_delimiter(self):
        self.non_default_delimiter_template('a')

    def test_number_delimiter(self):
        self.non_default_delimiter_template('1')

    def custom_null_indicator_template(self, indicator):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testnullindicator (
                a int primary key,
                b text
            )""")
        insert_non_null = self.session.prepare("INSERT INTO testnullindicator (a, b) VALUES (?, ?)")
        execute_concurrent_with_args(self.session, insert_non_null,
                                     [(1, 'eggs'), (100, 'sausage')])
        insert_null = self.session.prepare("INSERT INTO testnullindicator (a) VALUES (?)")
        execute_concurrent_with_args(self.session, insert_null, [(2,), (200,)])

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testnullindicator TO '{name}'".format(name=tempfile.name)
        cmds += " WITH NULL = '{d}'".format(d=indicator)
        self.node1.run_cqlsh(cmds=cmds)

        results = list(self.session.execute("SELECT a, b FROM ks.testnullindicator"))
        results = [[indicator if value is None else value for value in row]
                   for row in results]

        self.assertCsvResultEqual(tempfile.name, results)

    def test_undefined_as_null_indicator(self):
        self.custom_null_indicator_template('undefined')

    def test_null_as_null_indicator(self):
        self.custom_null_indicator_template('null')

    def test_writing_use_header(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testheader (
                a int primary key,
                b int
            )""")
        insert_statement = self.session.prepare("INSERT INTO testheader (a, b) VALUES (?, ?)")
        args = [(1, 10), (2, 20), (3, 30)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        cmds = "COPY ks.testheader TO '{name}'".format(name=tempfile.name)
        cmds += " WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        with open(tempfile.name, 'r') as csvfile:
            csv_values = list(csv.reader(csvfile))

        self.assertEqual(len(csv_values), 4, msg=str(csv_values))
        self.assertSequenceEqual(csv_values,
                                 [['a', 'b'], ['1', '10'], ['2', '20'], ['3', '30']])

    def test_reading_use_header(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testheader (
                a int primary key,
                b int
            )""")

        tempfile = NamedTemporaryFile()

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(tempfile.name, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['a', 'b'])
            writer.writeheader()
            for a, b in data:
                writer.writerow({'a': a, 'b': b})

        cmds = "COPY ks.testheader FROM '{name}'".format(name=tempfile.name)
        cmds += " WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        result = self.session.execute("SELECT * FROM testheader")
        self.assertItemsEqual([tuple(d) for d in data],
                              [tuple(r) for r in rows_to_list(result)])

    def test_explicit_column_order_writing(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testorder (
                a int primary key,
                b int,
                c text
            )""")

        data = [[1, 20, 'ham'], [2, 40, 'eggs'],
                [3, 60, 'beans'], [4, 80, 'toast']]
        insert_statement = self.session.prepare("INSERT INTO testorder (a, b, c) VALUES (?, ?, ?)")
        execute_concurrent_with_args(self.session, insert_statement, data)

        tempfile = NamedTemporaryFile()

        self.node1.run_cqlsh(
            "COPY ks.testorder (a, c, b) TO '{name}'".format(name=tempfile.name))

        reference_file = NamedTemporaryFile()
        with open(reference_file.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])

        assert_csvs_items_equal(tempfile.name, reference_file.name)

    def test_explicit_column_order_reading(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testorder (
                a int primary key,
                b text,
                c int
            )""")

        data = [[1, 20, 'ham'], [2, 40, 'eggs'],
                [3, 60, 'beans'], [4, 80, 'toast']]

        tempfile = NamedTemporaryFile()
        write_rows_to_csv(tempfile.name, data)

        self.node1.run_cqlsh(
            "COPY ks.testorder (a, c, b) FROM '{name}'".format(name=tempfile.name))

        results = list(self.session.execute("SELECT * FROM testorder"))
        reference_file = NamedTemporaryFile()
        with open(reference_file.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])

        self.assertCsvResultEqual(reference_file.name, results)

    def test_quoted_column_names_reading(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testquoted (
                "IdNumber" int PRIMARY KEY,
                "select" text
            )""")

        data = [[1, 'no'], [2, 'Yes'],
                [3, 'True'], [4, 'false']]

        tempfile = NamedTemporaryFile()
        write_rows_to_csv(tempfile.name, data)

        self.node1.run_cqlsh(
            """COPY ks.testquoted ("IdNumber", "select") FROM '{name}'""".format(name=tempfile.name))

        results = list(self.session.execute("SELECT * FROM testquoted"))
        self.assertCsvResultEqual(tempfile.name, results)

    def test_quoted_column_names_writing(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testquoted (
                "IdNumber" int PRIMARY KEY,
                "select" text
            )""")

        data = [[1, 'no'], [2, 'Yes'],
                [3, 'True'], [4, 'false']]
        insert_statement = self.session.prepare("""INSERT INTO testquoted ("IdNumber", "select") VALUES (?, ?)""")
        execute_concurrent_with_args(self.session, insert_statement, data)

        tempfile = NamedTemporaryFile()
        self.node1.run_cqlsh(
            """COPY ks.testquoted ("IdNumber", "select") TO '{name}'""".format(name=tempfile.name))

        reference_file = NamedTemporaryFile()
        write_rows_to_csv(reference_file.name, data)

        assert_csvs_items_equal(tempfile.name, reference_file.name)

    def data_validation_on_read_template(self, load_as_int, expect_invalid):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testvalidate (
                a int PRIMARY KEY,
                b int
            )""")

        data = [[1, load_as_int]]

        tempfile = NamedTemporaryFile()
        write_rows_to_csv(tempfile.name, data)

        cmd = """COPY ks.testvalidate (a, b) FROM '{name}'""".format(name=tempfile.name)
        out, err = self.node1.run_cqlsh(cmd, return_output=True)
        results = list(self.session.execute("SELECT * FROM testvalidate"))

        if expect_invalid:
            self.assertRegexpMatches('Bad [Rr]equest', err)
            self.assertFalse(results)
        else:
            self.assertFalse(err)
            self.assertCsvResultEqual(tempfile.name, results)

    def test_read_valid_data(self):
        # make sure the template works properly
        self.data_validation_on_read_template(2, expect_invalid=False)

    def test_read_invalid_float(self):
        self.data_validation_on_read_template(2.14, expect_invalid=True)

    def test_read_invalid_uuid(self):
        self.data_validation_on_read_template(uuid4(), expect_invalid=True)

    def test_read_invalid_text(self):
        self.data_validation_on_read_template('test', expect_invalid=True)

    # The next two tests fail due to differences in cqlsh formatting. At
    # initialization time, cqlsh monkey-patches the Cassandra driver so blobs
    # are decoded differently. If this changes, un-skip these tests. In the
    # meantime, the round-trip test for all datatypes shows that COPY works,
    # though the contents of the CSV aren't defined in cqlshlib.
    @unittest.skip('fails due to formatting differences in and out of cqlsh')
    def test_all_datatypes_write(self):
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, self.data)

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype TO '{name}'".format(name=tempfile.name))

        results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.assertCsvResultEqual(tempfile.name, results)

    @unittest.skip('fails due to formatting differences in and out of cqlsh')
    def test_all_datatypes_read(self):
        self.all_datatypes_prepare()

        tempfile = NamedTemporaryFile()
        with open(tempfile.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(self.data)

        debug('Importing from csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype FROM '{name}'".format(name=tempfile.name))

        results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.assertCsvResultEqual(tempfile.name, results)

    def test_all_datatypes_round_trip(self):
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""")
        self.session.execute(insert_statement, self.data)

        tempfile = NamedTemporaryFile()
        debug('Exporting to csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype TO '{name}'".format(name=tempfile.name))

        exported_results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.session.execute('TRUNCATE testdatatype')
        self.node1.run_cqlsh(cmds="COPY ks.testdatatype FROM '{name}'".format(name=tempfile.name))

        imported_results = list(self.session.execute("SELECT * FROM testdatatype"))

        self.assertEqual(exported_results, imported_results)

    def test_wrong_number_of_columns(self):
        self.prepare()
        self.session.execute("""
            CREATE TABLE testcolumns (
                a int PRIMARY KEY
            )""")

        data = [[1, 2]]
        tempfile = NamedTemporaryFile()
        with open(tempfile.name, 'w') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(data)

        debug('Importing from csv file: {name}'.format(name=tempfile.name))
        out, err = self.node1.run_cqlsh("COPY ks.testcolumns FROM '{name}'".format(name=tempfile.name),
                                        return_output=True)

        self.assertFalse(err)


class RoundTripTest(_CqlshCopyBase):

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

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        debug('Importing from csv file: {name}'.format(name=tempfile.name))
        self.node1.run_cqlsh(cmds="COPY ks.testcopyto FROM '{name}'".format(name=tempfile.name))
        new_results = list(self.session.execute("SELECT * FROM testcopyto"))
        self.assertEqual(results, new_results)


def write_rows_to_csv(filename, data):
    with open(filename, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for row in data:
            writer.writerow(row)
