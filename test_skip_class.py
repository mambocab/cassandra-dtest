from dtest import Tester
from tools import since


@since('1')
class TestUnskippedClass(Tester):
    def test_should_not_be_skipped(self):
        assert True

    def helper_should_not_run(self):
        assert False


@since('4')
class TestSkippedClass(Tester):
    def test_should_be_skipped(self):
        assert False

    def helper_should_not_run(self):
        assert False


class TestSinceStillWorksOnMethods(Tester):
    def test_should_not_be_skipped(self):
        assert True

    def helper_should_not_run(self):
        assert False

    @since('4')
    def test_should_be_skipped(self):
        assert False
