from dtest import Tester, require


@require(8099)
class TestNewRowFormat(Tester):
    """
    @jira_ticket 8099

    Tests that the new row format's desirable properties hold.

    The new row format (at least as documented
    [here](https://github.com/pcmanus/cassandra/blob/8099/guide_8099.md))
    should always be smaller than the old, there are two formats, "sparse" and "dense".
    Which format to use is chosen based on statistics over the rows being
    written to an SSTable.

    To simulate graph datasets, the data used for these tests should have
    (10s of? 100s of?) thousands of columns, but with fewer than 50 rows
    populated per column.

    @note We don't know yet if cassandra-stress is a useful data generator
    for these tests. It seems that some features would have
    to be added -- in particular, some control over the likelihood that a
    given column will be written, or the sparseness of rows.

    @note We don't know how the new row format affects schema changes.
    (Honestly, I don't know what schema changes look like under the hood now.)
    """

    def dense_sstables_smaller_test(self):
        """
        Test that SSTables are smaller in the 3.0 representation than in the old by:

        - on both a 3.0 and 2.2 cluster:
            - write data to cluster (same data)
            - flush all data in the cluster.

        The dataset should be one that will be represented in the "dense"
        variant of the new format.

        The total on-disk size of the data on the 3.0 cluster should be smaller.
        """

    def sparse_sstables_smaller_test(self):
        """
        Test that SSTables written in the 3.0 representation with sparse rows
        are smaller than in the old by:

        - on both a 3.0 and 2.2 cluster:
            - write data to cluster (same data)
            - flush all data in the cluster.

        The dataset should be one that will be represented in the "sparse"
        variant of the new format.

        The total on-disk size of the data on the 3.0 cluster should be smaller.
        """

    def compaction_speed_test(self):
        """
        @test_assumptions spinning storage media

        Quantify the speed differences in compaction in the 3.0 representation by:

        - on both a 3.0 and 2.2 cluster:
            - write data to cluster (same data)
            - flush all data in the cluster,
            - run compaction.

        The newer representation requires more seeks and will likely be slower
        on spinning disks (or would be slower, if nothing else changed between
        2.2 and 3.0).

        This should probably be measured with:

        - datasets represented in the sparse and dense variants of the new format
        - all compaction strategies.

        This test should be run on spinning storage media.
        """
