from dtest import Tester
from tools import since


@since("4")
class SkippedClass(Tester):
    def failing_test(self):
        assert False
