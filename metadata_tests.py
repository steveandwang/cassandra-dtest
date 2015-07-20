import time
import threading

from dtest import Tester
from tools import require


class TestMetadata(Tester):

    def __init__(self, *args, **kwargs):
        kwargs['cluster_options'] = {'start_rpc': 'true'}
        Tester.__init__(self, *args, **kwargs)

    def force_compact(self):
        cluster = self.cluster
        (node1, node2) = cluster.nodelist()
        if cluster.version() < "2.1":
            node1.nodetool("compact Keyspace1 Standard1")
        else:
            node1.nodetool("compact keyspace1 standard1")

    def force_repair(self):
        cluster = self.cluster
        (node1, node2) = cluster.nodelist()
        if cluster.version() < "2.1":
            node1.nodetool('repair Keyspace1 Standard1')
        else:
            node1.nodetool('repair keyspace1 standard1')

    def do_read(self):
        cluster = self.cluster
        (node1, node2) = cluster.nodelist()

        if cluster.version() < "2.1":
            node1.stress(['-o', 'read', '-n', '30000', '-l', '2', '-t', '1', '-I', 'LZ4Compressor'])
        else:
            node1.stress(['read', 'no-warmup', 'n=30000', '-schema', 'replication(factor=2)', 'compression=LZ4Compressor',
                              '-rate', 'threads=1'])

    @require(9831, broken_in='2.0')
    def metadata_reset_while_compact_test(self):
        """
        Resets the schema while a compact, read and repair happens.
        All kinds of glorious things can fail.
        """

        # while the schema is being reset, there will inevitably be some
        # queries that will error with this message
        self.ignore_log_patterns = '.*Unknown keyspace/cf pair.*'

        cluster = self.cluster
        cluster.populate(2).start(wait_other_notice=True)
        (node1, node2) = cluster.nodelist()

        node1.nodetool("disableautocompaction")
        node1.nodetool("setcompactionthroughput 1")

        for i in range(3):
            if cluster.version() < "2.1":
                node1.stress(['-o', 'insert', '-n', '30000', '-l', '2', '-t', '5', '-I', 'LZ4Compressor'])
            else:
                node1.stress(['write', 'no-warmup', 'n=30000', '-schema', 'replication(factor=2)', 'compression=LZ4Compressor',
                              '-rate', 'threads=5', '-pop', 'seq=1..30000'])
            node1.flush()

        thread = threading.Thread(target=self.force_compact)
        thread.start()
        time.sleep(1)

        thread2 = threading.Thread(target=self.force_repair)
        thread2.start()
        time.sleep(5)

        thread3 = threading.Thread(target=self.do_read)
        thread3.start()
        time.sleep(5)

        node1.nodetool("resetlocalschema")

        thread.join()
        thread2.join()
        thread3.join()
