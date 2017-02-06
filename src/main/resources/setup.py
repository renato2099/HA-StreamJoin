#!/usr/bin/env python
import sys
import signal
import os
import time
import logging

from observer import *
from threaded_ssh import ThreadedClients

class Config:
    exec_path   = ''
    num_parts   = 16
    sf          = 1
    tuples_sf   = 5
    bid_ratio   = 2
    zkdir       = '/tmp/zookeeper'
    kafkadir    = '/tmp/kafka-logs'
    server      = 'localhost'

if 'threading' in sys.modules:
        del sys.modules['threading']
        import gevent
        import gevent.socket
        import gevent.monkey
        gevent.monkey.patch_all()

logging.basicConfig()

def confZk():
    deleteClient = ThreadedClients([Storage.master], "rm -rf {0}".format(Config.zkdir), root=True)
    deleteClient.start()
    deleteClient.join()

def startZk():
    observer = Observer("binding to port")
    zk_cmd = '{0}/bin/zkServer.sh start-foreground'.format(Config.exec_path)
    zkClient = ThreadedClients([Config.server], zk_cmd, root=True, observers=[observer])
    zkClient.start()
    observer.waitFor(1)
    return zkClient

def startKafka():
    zkClient = startZk()
    # start kafka
    # create topics
    return [zkClient]

def exitGracefully(storageClients, signal, frame):
    print ""
    print "\033[1;31mShutting down Storage\033[0m"
    for client in storageClients:
        client.kill()
    unmount_memfs()
    exit(0)

if __name__ == "__main__":
    expClients = startKafka()
    signal.signal(signal.SIGINT, partial(exitGracefully, expClients))
    print "\033[1;31mExp started\033[0m"
    print "\033[1;31mHit Ctrl-C to shut it down\033[0m"
    for client in expClients:
        client.join()