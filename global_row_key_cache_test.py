import time

from dtest import Tester, debug

from cassandra.concurrent import execute_concurrent_with_args


class TestGlobalRowKeyCache(Tester):

    def functional_test(self):
        cluster = self.cluster
        cluster.populate(3)
        node1 = cluster.nodelist()[0]

        for keycache_size in (0, 10):
            for rowcache_size in (0, 10):
                debug("Testing with keycache size of %d MB, rowcache size of %d MB " %
                      (keycache_size, rowcache_size))
                keyspace_name = 'ks_%d_%d' % (keycache_size, rowcache_size)

                # make the caches save every five seconds
                cluster.set_configuration_options(values={
                    'key_cache_size_in_mb': keycache_size,
                    'row_cache_size_in_mb': rowcache_size,
                    'row_cache_save_period': 5,
                    'key_cache_save_period': 5,
                })

                cluster.start()
                session = self.patient_cql_connection(node1)

                self.create_ks(session, keyspace_name, rf=3)

                session.set_keyspace(keyspace_name)
                session.execute("CREATE TABLE test (k int PRIMARY KEY, v1 int, v2 int)")
                session.execute("CREATE TABLE test_clustering (k int, v1 int, v2 int, PRIMARY KEY (k, v1))")
                session.execute("CREATE TABLE test_counter (k int PRIMARY KEY, v1 counter)")
                session.execute("CREATE TABLE test_counter_clustering (k int, v1 int, v2 counter, PRIMARY KEY (k, v1))")

                # insert 100 rows into each table
                for cf in ('test', 'test_clustering'):
                    execute_concurrent_with_args(
                        session, session.prepare("INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)" % (cf,)),
                        [(i, i, i) for i in range(100)])

                execute_concurrent_with_args(
                    session, session.prepare("UPDATE test_counter SET v1 = v1 + ? WHERE k = ?"),
                    [(i, i) for i in range(100)],
                    concurrency=2)

                execute_concurrent_with_args(
                    session, session.prepare("UPDATE test_counter_clustering SET v2 = v2 + ? WHERE k = ? AND v1 = ?"),
                    [(i, i, i) for i in range(100)],
                    concurrency=2)

                # flush everything to get it into sstables
                for node in cluster.nodelist():
                    node.flush()

                # update the first 10 rows in every table
                # on non-counter tables, delete the first (remaining) row each round
                num_updates = 10
                for validation_round in range(3):
                    session.execute("DELETE FROM test WHERE k = %s", (validation_round,))
                    execute_concurrent_with_args(
                        session, session.prepare("UPDATE test SET v1 = ?, v2 = ? WHERE k = ?"),
                        [(i, validation_round, i) for i in range(validation_round + 1, num_updates)])

                    session.execute("DELETE FROM test_clustering WHERE k = %s AND v1 = %s", (validation_round, validation_round))
                    execute_concurrent_with_args(
                        session, session.prepare("UPDATE test_clustering SET v2 = ? WHERE k = ? AND v1 = ?"),
                        [(validation_round, i, i) for i in range(validation_round + 1, num_updates)])

                    execute_concurrent_with_args(
                        session, session.prepare("UPDATE test_counter SET v1 = v1 + ? WHERE k = ?"),
                        [(1, i) for i in range(num_updates)],
                        concurrency=2)

                    execute_concurrent_with_args(
                        session, session.prepare("UPDATE test_counter_clustering SET v2 = v2 + ? WHERE k = ? AND v1 = ?"),
                        [(1, i, i) for i in range(num_updates)],
                        concurrency=2)

                    self._validate_values(session, num_updates, validation_round)

                session.shutdown()

                # let the data be written to the row/key caches.
                debug("Letting caches be saved to disk")
                time.sleep(10)
                debug("Stopping cluster")
                cluster.stop()
                time.sleep(1)
                debug("Starting cluster")
                cluster.start()
                time.sleep(5)  # read the data back from row and key caches

                session = self.patient_cql_connection(node1)
                session.set_keyspace(keyspace_name)

                # check all values again
                self._validate_values(session, num_updates, validation_round=2)

    def _validate_values(self, session, num_updates, validation_round):
        # check values of non-counter tables
        for cf in ('test', 'test_clustering'):
            rows = list(session.execute("SELECT * FROM %s" % (cf,)))

            # one row gets deleted each validation round
            self.assertEquals(100 - (validation_round + 1), len(rows))

            # adjust enumeration start to account for row deletions
            for i, row in enumerate(sorted(rows), start=(validation_round + 1)):
                self.assertEquals(i, row.k)
                self.assertEquals(i, row.v1)

                # updated rows will have different values
                expected_value = validation_round if i < num_updates else i
                self.assertEquals(expected_value, row.v2)

        # check values of counter tables
        rows = list(session.execute("SELECT * FROM test_counter"))
        self.assertEquals(100, len(rows))
        for i, row in enumerate(sorted(rows)):
            self.assertEquals(i, row.k)

            # updated rows will get incremented once each round
            expected_value = i
            if i < num_updates:
                expected_value += validation_round + 1

            self.assertEquals(expected_value, row.v1)

        rows = list(session.execute("SELECT * FROM test_counter_clustering"))
        self.assertEquals(100, len(rows))
        for i, row in enumerate(sorted(rows)):
            self.assertEquals(i, row.k)
            self.assertEquals(i, row.v1)

            expected_value = i
            if i < num_updates:
                expected_value += validation_round + 1

            self.assertEquals(expected_value, row.v2)
