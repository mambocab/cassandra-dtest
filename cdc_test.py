from __future__ import division
from dtest import Tester

"""
- We want to add dtests to determine that CDC data is correctly flushed to
  cdc_raw and not discarded. Our basic goal is to write dataset A to CDC tables
  and dataset B to non-CDC tables, flush, then check that a superset of data A
  is available to a client in cdc_raw. We can do this by:
    - writing a mixed CDC/non-CDC dataset, tracking all data that should be
      exposed via CDC,
    - flushing,
    - saving off the contents of cdc_raw,
    - shutting down the cluster,
    - starting a new cluster,
    - initializing schema as necessary,
    - shutting down the new cluster,
    - moving the saved cdc_raw contents to commitlog directories,
    - starting the new cluster to commitlog replay, then
    - asserting all data that should have been exposed via CDC is in the
      table(s) that have been written to tables in the new cluster.
- We need tests of commitlog discard behavior when cdc_raw is full. In
  particular:
- once it reaches its maximum configured size, CDC tables should start
  rejecting writes, while CDC tables should still accept them.
- any new commitlog segments written after cdc_raw has reached its maximum
  configured size should be deleted, not moved to cdc_raw, on commitlog
  discard.
"""


class TestCDC(Tester):
    """
    @jira_ticket CASSANDRA-8844

    Test the correctness of some features of CDC, Change Data Capture, which
    provides an view of the commitlog on tables for which it is enabled.
    """

    def test_tables_readable_round_trip(self):
        """
        - We need basic tests to determine that commitlogs written with CDC enabled
          can be read when the CDC table has CDC disabled, and vice-versa.
        """
        self.cluster.populate(1)
         q
