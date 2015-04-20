from dtest import Tester, debug, DISABLE_VNODES
from ccmlib.node import Node, NodeError, TimeoutError
from cassandra import ConsistencyLevel, Unavailable, ReadTimeout
from cassandra.query import SimpleStatement
from tools import since, InterruptBootstrap
import time


class TestHintedHandoff(Tester):

    def interrupted_bootstrap_test(self):
        """Test interrupting bootstrap on hint-destined node"""

        cluster = self.cluster
        cluster.populate(2).start()
        [node1, node2] = cluster.nodelist()

        #create ks and table with rf 2
        cursor = self.patient_cql_connection(node1)
        ksq = "CREATE KEYSPACE hhtest WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':2}"
        cfq = "CREATE TABLE hhtest.hhtab(key int primary key, val int);"
        cursor.execute(ksq)
        cursor.execute(cfq)

        #stop node2 to allow hints to accumulate
        node2.stop(wait_other_notice=True, gently=False)

        #write data to generate hints
        numhints=100
        for x in range(0,numhints):
            insq = "INSERT INTO hhtest.hhtab(key,val) VALUES ({key}, {key})".format(key=str(x))
            cursor.execute(insq)

        #check hints table to ensure hints are properly generated
        hintcount = cursor.execute("select count(*) from system.hints;")
        self.assertTrue(hintcount[0][0] == numhints)

        # start node2 but kill in the process of bootstrapping to see if can cause Missing Host Id error
        failline = "Starting up server gossip"
        t = InterruptBootstrap(node2, message=failline)
        t.start()
        try:
            node2.start()
        except Exception, e:
            debug(str(e))
        t.join()

        time.sleep(5)

        #start properly to deliver hints
        try:
            node2.start(wait_other_notice=True)
        except Exception, e:
            debug(e)

        try:
            node2.watch_log_for_alive(node1, node2.mark_log(), timeout=10)
        except Exception, e:
            debug(e)

        #check hints delivered
        hintcount = cursor.execute("select count(*) from system.hints;")
        self.assertTrue(hintcount[0][0]==0)

        #verify hints delivered
        for x in range(0, numhints):
            query = SimpleStatement("SELECT val from hhtest.hhtab WHERE key={key};".format(key=str(x)), consistency_level=ConsistencyLevel.TWO)
            results = cursor.execute(query)
            self.assertEqual(results[0][0], x)
