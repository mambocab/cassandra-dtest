import os
import tempfile

from dtest import Tester, debug
from tools import rows_to_list, since


@since('0', '2.2.X')
class TestJson(Tester):

    def json_tools_test(self):

        debug("Starting cluster...")
        cluster = self.cluster
        cluster.set_configuration_options(batch_commitlog=True)
        cluster.populate(1).start()

        debug("Version: " + cluster.version())

        debug("Getting CQLSH...")
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        debug("Inserting data...")
        self.create_ks(session, 'Test', 1)

        session.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)

        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('frodo', 'pass@', 'male', 'CA', 1985);")
        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('sam', '@pass', 'male', 'NY', 1980);")

        res = session.execute("SELECT * FROM Test. users")

        self.assertItemsEqual(rows_to_list(res),
                              [[u'frodo', 1985, u'male', u'pass@', u'CA'],
                               [u'sam', 1980, u'male', u'@pass', u'NY']])

        debug("Flushing and stopping cluster...")
        node1.flush()
        cluster.stop()

        debug("Exporting to JSON file...")
        json_path = tempfile.mktemp(suffix='.schema.json')
        with open(json_path, 'w') as f:
            node1.run_sstable2json(f)

        if self.cluster.version() >= '2.1':
            with open(json_path, 'r') as fin:
                data = fin.read().splitlines(True)
            if data[0][0] == 'W':
                with open(json_path, 'w') as fout:
                    fout.writelines(data[1:])

        debug("Deleting cluster and creating new...")
        cluster.clear()
        cluster.start()

        debug("Inserting data...")
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'Test', 1)

        session.execute("""
            CREATE TABLE users (
                user_name varchar PRIMARY KEY,
                password varchar,
                gender varchar,
                state varchar,
                birth_year bigint
            );
        """)

        session.execute("INSERT INTO Test. users (user_name, password, gender, state, birth_year) VALUES('gandalf', 'p@$$', 'male', 'WA', 1955);")
        node1.flush()
        cluster.stop()

        debug("Importing JSON file...")
        with open(json_path) as f:
            node1.run_json2sstable(f, "test", "users")
        os.remove(json_path)

        debug("Verifying import...")
        cluster.start()
        [node1] = cluster.nodelist()
        session = self.patient_cql_connection(node1)

        res = session.execute("SELECT * FROM Test. users")

        debug("data: " + str(res))

        self.assertItemsEqual(rows_to_list(res),
                              [[u'frodo', 1985, u'male', u'pass@', u'CA'],
                               [u'sam', 1980, u'male', u'@pass', u'NY'],
                               [u'gandalf', 1955, u'male', u'p@$$', u'WA']])

    def test_json2sstable_round_trip(self):
        self.cluster.populate(1).start()
        node = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        session.execute("CREATE KEYSPACE ks with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        session.execute("""CREATE TABLE ks.tab (
                               col1 text,
                               col2 text,
                               col3 text,
                               col4 text,
                               PRIMARY KEY ((col1, col2), col3)
                           ) WITH CLUSTERING ORDER BY (col3 ASC)
                        """)
        session.execute("INSERT INTO ks.tab (col1, col2, col3, col4) VALUES ('a', 'b', 'c', 'd');")
        node.flush()

        json_path = tempfile.mktemp(suffix='.schema.json')
        with open(json_path, 'w') as f:
            node.run_sstable2json(f)
        with open(json_path, 'r') as f:
            debug('lolololololol')
            debug(f.read())

        with open(json_path, 'r') as f:
            node.run_json2sstable(f, 'ks', 'tab')

        os.remove(json_path)

        # json_string = '''[
        #                    {"key": "This is col1:This is col2",
        #                     "cells": [["This is col3:", "", 1443217787319002],
        #                               ["This is col3:"col4","This is col4",1443217787319002]]}
        #                  ]
        #               '''

        # self.json_source = tempfile.NamedTemporaryFile(delete=False)
        # self.json_source.write(json_string)

        # node.run_json2sstable(self.json_source, 'ks', 'tab')

        # os.remove(self.json_source.name)
