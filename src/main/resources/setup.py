#!/usr/bin/env python
import sys, traceback
import signal
import os
import time

from threaded_ssh import ThreadedClients
from observer import *
from exp_config import *

from functools import partial

import logging

if 'threading' in sys.modules:
        del sys.modules['threading']
        import gevent
        import gevent.socket
        import gevent.monkey
        gevent.monkey.patch_all()

logging.basicConfig()

def confZk():
    # TODO configure zk properties file?
    deleteClient = ThreadedClients([Config.server], "rm -rf {0}".format(Config.zk_dir), root=True)
    deleteClient.start()
    deleteClient.join()

def startZk():
    observer = Observer("binding to port")
    zk_cmd = '{0}/bin/zookeeper-server-start.sh {0}/config/{1}'.format(Config.exec_path, Config.zk_config)
    zkClient = ThreadedClients([Config.server], zk_cmd, root=True, observers=[observer])
    print zk_cmd
    zkClient.start()
    observer.waitFor(1)
    return zkClient

def startKafka():
    # remove data folder
    deleteClient = ThreadedClients([Config.server], "rm -rf {0}".format(Config.k_dir), root=True)
    deleteClient.start()
    deleteClient.join()

    # Start Kafka
    observer = Observer("started (kafka.server.KafkaServer)")
    k_cmd = '{0}/bin/kafka-server-start.sh {0}/config/{1}'.format(Config.exec_path, Config.k_config)
    kClient = ThreadedClients([Config.server], k_cmd, root=True, observers=[observer])
    kClient.start()
    observer.waitFor(1)
    return kClient

def startSetup():
    # start zk
    zkClient = startZk()
    # start kafka
    kafkaClient = startKafka()
    # create topics
    createTopic('bid-topic', Config.partitions, Config.exec_path, Config.server)
    createTopic('auction-topic', Config.partitions, Config.exec_path, Config.server)    
    return [kafkaClient] + [zkClient]

def stopSetup():
    # stop kafka
    stopZkClient = ThreadedClients([Config.server], "{0}/bin/zookeeper-server-stop.sh".format(Config.exec_path))
    stopZkClient.start()
    stopZkClient.join()
    # stop zk
    stopKafkaClient = ThreadedClients([Config.server], "{0}/bin/kafka-server-stop.sh".format(Config.exec_path))
    stopKafkaClient.start()
    stopKafkaClient.join()

def createTopic(tName, nParts, ePath, server):
    # TODO Change zk default port?
    ct_cmd = '{0}/bin/kafka-topics.sh --create --partitions {1} --zookeeper {2}:2181 --replication-factor 1 --topic {3}'
    topicClient = ThreadedClients([Config.server], ct_cmd.format(ePath, nParts, server, tName), root=True)
    topicClient.start()
    topicClient.join()

def exitGracefully(storageClients, signal, frame):
    try:
        print ""
        print "\033[1;31mShutting down Storage\033[0m"
        for client in storageClients:
            client.kill()
        #unmount_memfs()
        exit(0)
    except KeyboardInterrupt:
        print "\033[1;31mExiting"
    except Exception:
        traceback.print_exc(file=sys.stdout)
    except SystemExit as e:
        print "\033[1;31mExiting\033[0m"
        #exit(e)
    #exit(0)

if __name__ == "__main__":
    expClients = startSetup()
    signal.signal(signal.SIGINT, partial(exitGracefully, expClients))
    print "\033[1;31mExp started\033[0m"
    print "\033[1;31mHit Ctrl-C to shut it down\033[0m"
    for client in expClients:
        client.join()
