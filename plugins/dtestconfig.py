from collections import namedtuple
from nose import plugins


GlobalConfigObject = namedtuple('GlobalConfigObject', [
    'vnodes',  # disable or enable vnodes
])

_CONFIG = None
UNCONFIGURED = object()


class DtestConfigPlugin(plugins.Plugin):
    """
    Pass in configuration options for the dtests via the command line.
    """
    enabled = True  # if this plugin is loaded at all, we're using it
    name = 'dtest_config'

    def __init__(self, config=None):
        self.CONFIG = config

    def options(self, parser, env):
        parser.add_option(
            '--no-vnodes',
            dest='vnodes',
            action='store_false',
            default=UNCONFIGURED,
            help='If set, disable vnodes.',
        )

    def configure(self, options, conf):
        if self.config is None:
            self.CONFIG = GlobalConfigObject(
                vnodes=options.vnodes,
            )

        global _CONFIG
        self.CONFIG = _CONFIG
