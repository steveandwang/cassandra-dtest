from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since, InterruptBootstrap
from pyassertions import *
import time


class TestSystemPeers(Tester):

    def simple_bootstrap_test(self):
        """Start up nodes in a cluster one by one and determine if other nodes are seen in peers"""
        cluster = self.cluster
        cluster.populate(3)
        [node1,node2, node3] = cluster.nodelist()

        node1.start()
        cursor = self.patient_cql_connection(node1)

        node2.start(wait_other_notice=True)
        cursor2 = self.patient_cql_connection(node2)

        assert_all(cursor, "select peer from system.peers", [['127.0.0.2']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1']])

        node3.start(wait_other_notice=True)
        cursor3 = self.patient_cql_connection(node3)

        assert_all(cursor, "select peer from system.peers", [['127.0.0.2'], ['127.0.0.3']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.3']])
        assert_all(cursor3, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.2']])

    def simple_remove_test(self):
        "Remove nodes from an active cluster one by one and determine correct changes made to peers"
        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()

        cursor = self.patient_cql_connection(node1)
        cursor2 = self.patient_cql_connection(node2)
        cursor3 = self.patient_cql_connection(node3)

        time.sleep(5)

        assert_all(cursor, "select peer from system.peers", [['127.0.0.2'], ['127.0.0.3']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.3']])
        assert_all(cursor3, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.2']])

        node3.stop(wait_other_notice=True)
        assert_all(cursor, "select peer from system.peers", [['127.0.0.2']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1']])

        node2.stop(wait_other_notice=True)
        assert_all(cursor, "select peer from system.peers", [['127.0.0.2']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1']])

    def replace_peers_test(self):
        """Insert data, stop and replace a node and check that correct addresses appear in peers"""
        debug("Starting cluster with 3 nodes.")
        cluster = self.cluster
        cluster.populate(3).start()
        [node1,node2, node3] = cluster.nodelist()
        
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
        
        cursor = self.patient_cql_connection(node1)
        stress_table = 'keyspace1.standard1' if self.cluster.version() >= '2.1' else '"Keyspace1"."Standard1"'
        query = SimpleStatement('select * from %s LIMIT 1' % stress_table, consistency_level=ConsistencyLevel.THREE)
        initialData = cursor.execute(query)
        
        #stop node, query should not work with consistency 3
        debug("Stopping node 3.")
        node3.stop(wait_other_notice=True, gently=False)
        
        #replace node 3 with node 4
        debug("Starting node 4 to replace node 3")
        node4 = Node('node4', cluster, True, ('127.0.0.4', 9160), ('127.0.0.4', 7000), '7400', '0', None, ('127.0.0.4',9042))
        cluster.add(node4, False)
        node4.start(wait_other_notice=True, jvm_args=["-Dcassandra.replace_address_first_boot=127.0.0.3"])

        cursor2 = self.patient_cql_connection(node2)
        cursor4 = self.patient_cql_connection(node4)

        assert_all(cursor, "select peer from system.peers", [['127.0.0.2'], ['127.0.0.4']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.4']])
        assert_all(cursor4, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.2']])
        
        #query should work again
        debug("Verifying querying works again.")
        finalData = cursor.execute(query)
        self.assertListEqual(initialData, finalData)
        
        #restart node4 (if error's might have to change num_tokens)
        node4.stop(wait_other_notice=True, gently=False)
        #node4.set_configuration_options(values={'num_tokens': 1})
        node4.start(wait_other_notice=True)
        
        assert_all(cursor, "select peer from system.peers", [['127.0.0.2'], ['127.0.0.4']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.4']])
        assert_all(cursor4, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.2']])
        
        debug("Verifying querying works again.")
        finalData = cursor.execute(query)
        self.assertListEqual(initialData, finalData)

    def errored_bootstrap_test(self):
        """Test bootstrapping node then interrupting to ensure peers picks up on change"""

        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()

        #create ks and table with rf 2
        cursor = self.patient_cql_connection(node1)
        cursor2 = self.patient_cql_connection(node2)

        ksq = "CREATE KEYSPACE pbtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':2}"
        cfq = "CREATE TABLE pbtest.pbtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        #write data to validate
        numkeys=10000
        for x in range(0,numkeys):
            insq = "INSERT INTO pbtest.pbtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        node3 = Node('node3', cluster, True, ('127.0.0.3', 9160), ('127.0.0.3', 7000), '7300', '0', None, ('127.0.0.3',9042))
        cluster.add(node3, False)

        # start node3 but kill in the process of bootstrapping
        failline = "Starting Messaging Service"
        t = InterruptBootstrap(node2, message=failline)
        t.start()
        try:
            node3.start()
        except Exception, e:
            debug(str(e))
        t.join()
        cursor3 = self.patient_cql_connection(node3)


        #verify that peers only shows node1/node2 (since bootstrap stopped)
        time.sleep(5)
        assert_all(cursor, "select peer from system.peers", [['127.0.0.2']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1']])

        #start properly
        try:
            node3.start(wait_other_notice=True)
        except Exception, e:
            debug(str(e))

        #verify peers shows all three nodes
        assert_all(cursor, "select peer from system.peers", [['127.0.0.2'], ['127.0.0.3']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.3']])
        assert_all(cursor3, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.2']])

        #verify data
        for x in range(0, numkeys):
            query = SimpleStatement("SELECT val from pbtest.pbtab WHERE key={key};".format(key=str(x)), consistency_level=ConsistencyLevel.THREE)
            results = cursor.execute(query)
            self.assertEqual(results[0][0], x)

    def truncate_test(self):
        """Combines sequences of truncate and restarts"""
        cluster = self.cluster
        cluster.populate(3).start()
        [node1, node2, node3] = cluster.nodelist()
        cursor = self.patient_cql_connection(node1)
        cursor2 = self.patient_cql_connection(node2)
        cursor3 = self.patient_cql_connection(node1)

        cursor.execute("truncate system.peers")
        node1.stop()
        node1.start(wait_other_notice=True)
        
        cursor2.execute("truncate system.peers")
        node2.stop()
        node2.start(wait_other_notice=True)
        
        cursor3.execute("truncate system.peers")
        node3.stop()
        node3.start(wait_other_notice=True)
        time.sleep(2)

        assert_all(cursor, "select peer from system.peers", [['127.0.0.2'], ['127.0.0.3']])
        assert_all(cursor2, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.3']])
        assert_all(cursor3, "select peer from system.peers", [['127.0.0.1'], ['127.0.0.2']])
