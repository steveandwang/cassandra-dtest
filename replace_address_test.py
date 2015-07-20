from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since, InterruptBootstrap


class NodeUnavailable(Exception):
    pass


class TestReplaceAddress(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        # Ignore these log patterns:
        self.ignore_log_patterns = [
            # This one occurs when trying to send the migration to a
            # node that hasn't started yet, and when it does, it gets
            # replayed and everything is fine.
            r'Can\'t send migration request: node.*is down',
            # This is caused by starting a node improperly (replacing active/nonexistent)
            r'Exception encountered during startup',
            # This is caused by trying to replace a nonexistent node
            r'Exception in thread Thread',
            # ignore streaming error during bootstrap
            r'Streaming error occurred'
        ]
        Tester.__init__(self, *args, **kwargs)
        self.allow_log_errors = True

    def replace_stopped_node_test(self):
        """Check that the replace address function correctly replaces a node that has failed in a cluster.
        Create a cluster, cause a node to fail, and bring up a new node with the replace_address parameter.
        Check that tokens are migrated and that data is replicated properly.
        """
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            #a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

        debug("Inserting Data...")
        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=10000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        session.default_timeout = 45
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = session.execute(query)

        #stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=False, wait_other_notice=True)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
                session.execute(query)
            except (Unavailable, ReadTimeout):
                raise NodeUnavailable("Node could not be queried.")

        #replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(replace_address='127.0.0.3', wait_for_binary_proto=True)

        #query should work again
        debug("Verifying querying works again.")
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        finalData = session.execute(query)
        self.assertListEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

        #check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start()
        checkCollision = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

    def replace_active_node_test(self):

        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        #replace active node 3 with node 4
        debug("Starting node 4 to replace active node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4', 9042))
        cluster.add(node4, False)

        with self.assertRaises(NodeError):
            try:
                node4.start(replace_address='127.0.0.3', wait_for_binary_proto=True)
            except (NodeError, TimeoutError):
                raise NodeError("Node could not start.")

        checkError = node4.grep_log("java.lang.UnsupportedOperationException: Cannot replace a live node...")
        self.assertEqual(len(checkError), 1)

    def replace_nonexistent_node_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        debug('Start node 4 and replace an address with no node')
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4', 9042))
        cluster.add(node4, False)

        #try to replace an unassigned ip address
        with self.assertRaises(NodeError):
            try:
                node4.start(replace_address='127.0.0.5', wait_for_binary_proto=True)
            except (NodeError, TimeoutError):
                raise NodeError("Node could not start.")

    def replace_first_boot_test(self):
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        if DISABLE_VNODES:
            numNodes = 1
        else:
            # a little hacky but grep_log returns the whole line...
            numNodes = int(node3.get_conf_option('num_tokens'))

        debug(numNodes)

        debug("Inserting Data...")
        if cluster.version() < "2.1":
            node1.stress(['-o', 'insert', '--num-keys=10000', '--replication-factor=3'])
        else:
            node1.stress(['write', 'n=10000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = session.execute(query)

        # stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(gently=False)

        debug("Testing node stoppage (query should fail).")
        with self.assertRaises(NodeUnavailable):
            try:
                session.execute(query, timeout=30)
            except (Unavailable, ReadTimeout):
                raise NodeUnavailable("Node could not be queried.")

        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4', 9042))
        cluster.add(node4, False)
        node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"], wait_for_binary_proto=True)

        # query should work again
        debug("Verifying querying works again.")
        finalData = session.execute(query)
        self.assertListEqual(initialData, finalData)

        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

        # check that restarting node 3 doesn't work
        debug("Try to restart node 3 (should fail)")
        node3.start()
        checkCollision = node1.grep_log("between /127.0.0.3 and /127.0.0.4; /127.0.0.4 is the new owner")
        debug(checkCollision)
        self.assertEqual(len(checkCollision), 1)

        # restart node4 (if error's might have to change num_tokens)
        node4.stop(gently=False)
        node4.start(wait_for_binary_proto=True)

        debug("Verifying querying works again.")
        finalData = session.execute(query)
        self.assertListEqual(initialData, finalData)

        # we redo this check because restarting node should not result in tokens being moved again, ie number should be same
        debug("Verifying tokens migrated sucessfully")
        movedTokensList = node4.grep_log("Token .* changing ownership from /127.0.0.3 to /127.0.0.4")
        debug(movedTokensList[0])
        self.assertEqual(len(movedTokensList), numNodes)

    @since('2.2')
    def resumable_replace_test(self):
        """Test resumable bootstrap while replacing node"""

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = session.execute(query)

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4', 9042))
        cluster.add(node4, False)
        try:
            node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"])
        except NodeError:
            pass  # node doesn't start as expected
        t.join()

        # bring back node1 and invoke nodetool bootstrap to resume bootstrapping
        node1.start()
        node4.nodetool('bootstrap resume')
        # check if we skipped already retrieved ranges
        node4.watch_log_for("already available. Skipping streaming.")
        # wait for node3 ready to query
        node4.watch_log_for("Listening for thrift clients...")

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node4)
        rows = session.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

        #query should work again
        debug("Verifying querying works again.")
        finalData = session.execute(query)
        self.assertListEqual(initialData, finalData)

    @since('2.2')
    def replace_with_reset_resume_state_test(self):
        """Test replace with resetting bootstrap progress"""

        cluster = self.cluster
        cluster.populate(3).start()
        node1, node2, node3 = cluster.nodelist()

        node1.stress(['write', 'n=100000', '-schema', 'replication(factor=3)'])

        session = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = session.execute(query)

        node3.stop(gently=False)

        # kill node1 in the middle of streaming to let it fail
        t = InterruptBootstrap(node1)
        t.start()
        # replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4', 9042))
        cluster.add(node4, False)
        try:
            node4.start(jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"])
        except NodeError:
            pass  # node doesn't start as expected
        t.join()
        node1.start()

        # restart node4 bootstrap with resetting bootstrap state
        node4.stop()
        mark = node4.mark_log()
        node4.start(jvm_args=[
                    "-Dcassandra.replace_address_first_boot=127.0.0.3",
                    "-Dcassandra.reset_bootstrap_progress=true"
                   ])
        # check if we reset bootstrap state
        node4.watch_log_for("Resetting bootstrap progress to start fresh", from_mark=mark)
        # wait for node3 ready to query
        node4.watch_log_for("Listening for thrift clients...", from_mark=mark)

        # check if 2nd bootstrap succeeded
        session = self.exclusive_cql_connection(node4)
        rows = session.execute("SELECT bootstrapped FROM system.local WHERE key='local'")
        assert len(rows) == 1
        assert rows[0][0] == 'COMPLETED', rows[0][0]

        #query should work again
        debug("Verifying querying works again.")
        finalData = session.execute(query)
        self.assertListEqual(initialData, finalData)
