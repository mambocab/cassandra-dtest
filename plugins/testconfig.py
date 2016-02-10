from collections import namedtuple
from nose import plugins


GlobalConfigObject = namedtuple('GlobalConfigObject', [
    'vnodes',  # disable or enable vnodes
])

_CONFIG = None


class DtestConfigPlugin(plugins.Plugin):
    """
    Pass in configuration options for the dtests via the command line.
    """
    enabled = True  # if this plugin is loaded at all, we're using it
    name = 'dtest_config'

    def options(self, parser, env):
        parser.add_option(
            '--no-vnodes',
            dest='vnodes',
            action='store_false',
            default=True,
            help='If set, disable vnodes.',
        )

    def configure(self, options, conf):
        global _CONFIG
        _CONFIG = GlobalConfigObject(
            vnodes=options.vnodes,
        )
