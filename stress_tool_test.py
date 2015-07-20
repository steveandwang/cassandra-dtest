from __future__ import division

from dtest import Tester
from tools import rows_to_list, since


@since('3.0')
class TestStressSparsenessRatio(Tester):
    """
    @jira_ticket CASSANDRA-9522

    Tests for the `row-population-ratio` parameter to `cassandra-stress`.
    """
    def uniform_ratio_test(self):
        """
        Tests that the ratio-specifying string 'uniform(5..15)/50' results in
        ~80% of the values written being non-null.
        """
        self.distribution_template(ratio_spec='uniform(5..15)/50',
                                   expected_ratio=.8,
                                   delta=.1)

    def fixed_ratio_test(self):
        """
        Tests that the string 'fixed(1)/3' results in ~1/3 of the values
        written being non-null.
        """
        self.distribution_template(ratio_spec='fixed(1)/3',
                                   expected_ratio=1 - 1/3,
                                   delta=.01)

    def distribution_template(self, ratio_spec, expected_ratio, delta):
        """
        @param ratio_spec the string passed to `row-population-ratio` in the call to `cassandra-stress`
        @param expected_ratio the expected ratio of null/non-null values in the values written
        @param delta the acceptable delta between the expected and actual ratios

        A parameterized test for the `row-population-ratio` parameter to
        `cassandra-stress`.
        """
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        node = self.cluster.nodelist()[0]
        node.stress(['write', 'n=1000', '-rate', 'threads=50', '-col', 'n=FIXED(50)',
                     '-insert', 'row-population-ratio={ratio_spec}'.format(ratio_spec=ratio_spec)])
        session = self.patient_cql_connection(node)
        written = rows_to_list(session.execute('SELECT * FROM keyspace1.standard1;'))

        num_nones = sum(row.count(None) for row in written)
        num_results = sum(len(row) for row in written)

        self.assertAlmostEqual(float(num_nones) / num_results, expected_ratio, delta=delta)
