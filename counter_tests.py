from dtest import Tester
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

import random, time, uuid
from assertions import assert_invalid, assert_one
from tools import rows_to_list, since

class TestCounters(Tester):

    def simple_increment_test(self):
        """ Simple incrementation test (Created for #3465, that wasn't a bug) """
        cluster = self.cluster

        cluster.populate(3).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 3)
        self.create_cf(session, 'cf', validation="CounterColumnType", columns={'c': 'counter'})

        sessions = [self.patient_cql_connection(node, 'ks') for node in nodes]
        nb_increment = 50
        nb_counter = 10

        for i in xrange(0, nb_increment):
            for c in xrange(0, nb_counter):
                session = sessions[(i + c) % len(nodes)]
                query = SimpleStatement("UPDATE cf SET c = c + 1 WHERE key = 'counter%i'" % c, consistency_level=ConsistencyLevel.QUORUM)
                session.execute(query)

            session = sessions[i % len(nodes)]
            keys = ",".join(["'counter%i'" % c for c in xrange(0, nb_counter)])
            query = SimpleStatement("SELECT key, c FROM cf WHERE key IN (%s)" % keys, consistency_level=ConsistencyLevel.QUORUM)
            res = session.execute(query)

            assert len(res) == nb_counter
            for c in xrange(0, nb_counter):
                assert len(res[c]) == 2, "Expecting key and counter for counter%i, got %s" % (c, str(res[c]))
                assert res[c][1] == i + 1, "Expecting counter%i = %i, got %i" % (c, i + 1, res[c][0])

    def upgrade_test(self):
        """ Test for bug of #4436 """

        cluster = self.cluster

        cluster.populate(2).start()
        nodes = cluster.nodelist()

        session = self.patient_cql_connection(nodes[0])
        self.create_ks(session, 'ks', 2)

        query = """
            CREATE TABLE counterTable (
                k int PRIMARY KEY,
                c counter
            )
        """
        query = query +  "WITH compression = { 'sstable_compression' : 'SnappyCompressor' }"

        session.execute(query)
        time.sleep(2)

        keys = range(0, 4)
        updates = 50

        def make_updates():
            session = self.patient_cql_connection(nodes[0], keyspace='ks')
            upd = "UPDATE counterTable SET c = c + 1 WHERE k = %d;"
            batch = " ".join(["BEGIN COUNTER BATCH"] + [upd % x for x in keys] + ["APPLY BATCH;"])

            kmap = { "k%d" % i : i for i in keys }
            for i in range(0, updates):
                query = SimpleStatement(batch, consistency_level=ConsistencyLevel.QUORUM)
                session.execute(query)

        def check(i):
            session = self.patient_cql_connection(nodes[0], keyspace='ks')
            query = SimpleStatement("SELECT * FROM counterTable", consistency_level=ConsistencyLevel.QUORUM)
            rows = session.execute(query)

            assert len(rows) == len(keys), "Expected %d rows, got %d: %s" % (len(keys), len(rows), str(rows))
            for row in rows:
                assert row[1] == i * updates, "Unexpected value %s" % str(row)

        def rolling_restart():
            # Rolling restart
            for i in range(0, 2):
                time.sleep(.2)
                nodes[i].nodetool("drain")
                nodes[i].stop(wait_other_notice=False)
                nodes[i].start(wait_other_notice=True, wait_for_binary_proto=True)
                time.sleep(.2)

        make_updates()
        check(1)
        rolling_restart()

        make_updates()
        check(2)
        rolling_restart()

        make_updates()
        check(3)
        rolling_restart()

        check(3)

    def counter_consistency_test(self):
        """
        Do a bunch of writes with ONE, read back with ALL and check results.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 3)

        stmt = """
              CREATE TABLE counter_table (
              id uuid PRIMARY KEY,
              counter_one COUNTER,
              counter_two COUNTER,
              )
           """
        session.execute(stmt)

        counters = []
        # establish 50 counters (2x25 rows)
        for i in xrange(25):
            _id = str(uuid.uuid4())
            counters.append(
                {_id: {'counter_one':1, 'counter_two':1}}
            )

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_one = counter_one + 1, counter_two = counter_two + 1
                where id = {uuid}""".format(uuid=_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

        # increment a bunch of counters with CL.ONE
        for i in xrange(10000):
            counter = counters[random.randint(0, len(counters)-1)]
            counter_id = counter.keys()[0]

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_one = counter_one + 2
                where id = {uuid}""".format(uuid=counter_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_two = counter_two + 10
                where id = {uuid}""".format(uuid=counter_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_one = counter_one - 1
                where id = {uuid}""".format(uuid=counter_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

            query = SimpleStatement("""
                UPDATE counter_table
                SET counter_two = counter_two - 5
                where id = {uuid}""".format(uuid=counter_id), consistency_level=ConsistencyLevel.ONE)
            session.execute(query)

            # update expectations to match (assumed) db state
            counter[counter_id]['counter_one'] += 1
            counter[counter_id]['counter_two'] += 5

        # let's verify the counts are correct, using CL.ALL
        for counter_dict in counters:
            counter_id = counter_dict.keys()[0]

            query = SimpleStatement("""
                SELECT counter_one, counter_two
                FROM counter_table WHERE id = {uuid}
                """.format(uuid=counter_id), consistency_level=ConsistencyLevel.ALL)
            rows = session.execute(query)

            counter_one_actual, counter_two_actual = rows[0]

            self.assertEqual(counter_one_actual, counter_dict[counter_id]['counter_one'])
            self.assertEqual(counter_two_actual, counter_dict[counter_id]['counter_two'])

    def multi_counter_update_test(self):
        """
        Test for singlular update statements that will affect multiple counters.
        """
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 3)

        session.execute("""
            CREATE TABLE counter_table (
            id text,
            myuuid uuid,
            counter_one COUNTER,
            PRIMARY KEY (id, myuuid))
            """)

        expected_counts = {}

        # set up expectations
        for i in range(1,6):
            _id = uuid.uuid4()

            expected_counts[_id] = i

        for k, v in expected_counts.items():
            session.execute("""
                UPDATE counter_table set counter_one = counter_one + {v}
                WHERE id='foo' and myuuid = {k}
                """.format(k=k, v=v))

        for k, v in expected_counts.items():
            count = session.execute("""
                SELECT counter_one FROM counter_table
                WHERE id = 'foo' and myuuid = {k}
                """.format(k=k))

            self.assertEqual(v, count[0][0])

    def validate_empty_column_name_test(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("""
            CREATE TABLE compact_counter_table (
                pk int,
                ck text,
                value counter,
                PRIMARY KEY (pk, ck))
            WITH COMPACT STORAGE
            """)

        assert_invalid(session, "UPDATE compact_counter_table SET value = value + 1 WHERE pk = 0 AND ck = ''")
        assert_invalid(session, "UPDATE compact_counter_table SET value = value - 1 WHERE pk = 0 AND ck = ''")

        session.execute("UPDATE compact_counter_table SET value = value + 5 WHERE pk = 0 AND ck = 'ck'")
        session.execute("UPDATE compact_counter_table SET value = value - 2 WHERE pk = 0 AND ck = 'ck'")

        assert_one(session, "SELECT pk, ck, value FROM compact_counter_table", [0, 'ck', 3])

    @since('2.0')
    def drop_counter_column_test(self):
        """Test for CASSANDRA-7831"""
        cluster = self.cluster
        cluster.populate(1).start()
        node1, = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        self.create_ks(session, 'counter_tests', 1)

        session.execute("CREATE TABLE counter_bug (t int, c counter, primary key(t))")

        session.execute("UPDATE counter_bug SET c = c + 1 where t = 1")
        row = session.execute("SELECT * from counter_bug")

        self.assertEqual(rows_to_list(row)[0], [1, 1])
        self.assertEqual(len(row), 1)

        session.execute("ALTER TABLE counter_bug drop c")

        assert_invalid(session, "ALTER TABLE counter_bug add c counter", "Cannot re-add previously dropped counter column c")
