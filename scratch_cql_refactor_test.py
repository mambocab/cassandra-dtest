from __future__ import print_function

from abc import ABCMeta
from unittest import SkipTest
from dtest import Tester, debug


class DtestSpec(object):
    __metaclass__ = ABCMeta

    def __init__(self, tester):
        self.tester = tester

    def __call__(self):
        self._run()

    def _run(self):
        debug('self.prepare_cluster_config()')
        self.prepare_cluster_config()
        debug('self.prepare_cluster_state()')
        self.prepare_cluster_state()
        debug('self.prepare_tester_state(self.tester.cluster.nodelist()[0])')
        self.prepare_tester_state(self.tester.cluster.nodelist()[0])
        debug('self.validate()')
        self.validate()


class LargeCollectionErrorsSpec(DtestSpec):

    def prepare_cluster_config(self, ordered=False, create_keyspace=True,
                               use_cache=False, nodes=1, rf=1,
                               protocol_version=None, user=None, password=None,
                               start_rpc=False):

        cluster = self.tester.cluster

        if ordered:
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if use_cache:
            cluster.set_configuration_options(values={'row_cache_size_in_mb': 100})

        if start_rpc:
            cluster.set_configuration_options(values={'start_rpc': True})

        if user:
            config = {'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
                      'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
                      'permissions_validity_in_ms': 0}
            cluster.set_configuration_options(values=config)

        if not cluster.nodelist():
            debug('starting cluster')
            cluster.populate(nodes).start(wait_for_binary_proto=True)

    def prepare_cluster_state(self):
        session = self.tester.patient_cql_connection(self.tester.cluster.nodelist()[0])
        session.execute("DROP KEYSPACE IF EXISTS ks")
        self.tester.create_ks(session, 'ks', self.tester.RF)
        session.execute("""
            CREATE TABLE ks.maps (
                userid text PRIMARY KEY,
                properties map<int, text>
            );
        """)

    def prepare_tester_state(self, node):
        self.node = node

        version = self.node.get_cassandra_version()

        debug('version is {}'.format(version))
        if version >= '3.0':
            raise SkipTest('version {} not compatible with protocol version 2'.format(version))

        self.tester.ignore_log_patterns = ['Detected collection for table']

    def validate(self):
        # We only warn with protocol 2
        session = self.tester.patient_cql_connection(self.node, protocol_version=2)
        session.execute('USE ks')

        # Insert more than the max, which is 65535
        for i in range(70000):
            session.execute("UPDATE maps SET properties[{}] = 'x' WHERE userid = 'user'".format(i))

        # Query for the data and throw exception
        session.execute("SELECT properties FROM maps WHERE userid = 'user'")
        self.node.watch_log_for(
            'Detected collection for table ks.maps with 70000 elements, more than the 65535 limit. '
            'Only the first 65535 elements will be returned to the client. '
            'Please see http://cassandra.apache.org/doc/cql3/CQL.html#collections for more details.'
        )


class _SubTester(Tester):
    for spec in DtestSpec.__subclasses__():
        debug(spec)

        def _test_func(future_self, _spec=spec):
            try:
                _subclasses = future_self.spec_subclasses
            except AttributeError:
                _subclasses = []
            _new_spec_bases = tuple([_spec] + list(_subclasses))
            debug('_new_spec_bases: ' + str(_new_spec_bases))
            _new_spec_type = type('_Nonce', _new_spec_bases, {})
            debug('_new_spec_type mro: ' + str(_new_spec_type.mro()))
            _new_spec_type(future_self)()

        _future_test_name = spec.__name__ + '_test'
        _test_func.__name__ = _future_test_name
        locals()[_future_test_name] = _test_func


class CQLTest(_SubTester):
    RF = 1


class UpgradeTest(_SubTester):
    RF = 2

    class _UpgradeSubSpec(DtestSpec):
        def _run(self):
            self.prepare_cluster_config(nodes=2)
            self.prepare_cluster_state()
            self.prepare_tester_state(self.tester.cluster.nodelist()[0])
            self.validate()
            self.prepare_tester_state(self.tester.cluster.nodelist()[1])
            self.validate()

    spec_subclasses = (_UpgradeSubSpec,)
